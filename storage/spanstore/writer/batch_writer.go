package writer

import (
	"context"
	"github.com/hashicorp/go-hclog"
	"github.com/ydb-platform/jaeger-ydb-store/internal/connection_monitor"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"

	"github.com/ydb-platform/jaeger-ydb-store/schema"
	"github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/dbmodel"
	wmetrics "github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/writer/metrics"
)

const (
	tblTraces = "traces"
)

type BatchSpanWriter struct {
	metrics      batchWriterMetrics
	pool         table.Client
	logger       *zap.Logger
	pluginLogger hclog.Logger
	opts         BatchWriterOptions
}

func NewBatchWriter(pool table.Client, factory metrics.Factory, logger *zap.Logger, opts BatchWriterOptions) *BatchSpanWriter {
	pluginLogger := hclog.New(&hclog.LoggerOptions{
		Name:       "batch Writer",
		JSONFormat: true,
	})
	return &BatchSpanWriter{
		pool:         pool,
		logger:       logger,
		pluginLogger: pluginLogger,
		opts:         opts,
		metrics:      newBatchWriterMetrics(factory),
	}
}

func (w *BatchSpanWriter) WriteItems(items []interface{}) {
	parts := map[schema.PartitionKey][]*model.Span{}
	for _, item := range items {
		span := item.(*model.Span)
		k := schema.PartitionFromTime(span.StartTime)
		parts[k] = append(parts[k], span)
	}
	for k, partial := range parts {
		w.writeItemsToPartition(k, partial)
	}
}

func (w *BatchSpanWriter) writeItemsToPartition(part schema.PartitionKey, items []*model.Span) {
	spanRecords := make([]types.Value, 0, len(items))
	for _, span := range items {
		dbSpan, _ := dbmodel.FromDomain(span)
		spanRecords = append(spanRecords, dbSpan.StructValue())
	}

	ctx, ctxCancel := context.WithTimeout(context.Background(), w.opts.WriteTimeout)
	defer ctxCancel()
	tableName := func(table string) string {
		return part.BuildFullTableName(w.opts.DbPath.String(), table)
	}
	var err error

	if err = w.uploadRows(ctx, tableName(tblTraces), spanRecords, w.metrics.traces); err != nil {
		w.logger.Error("insertSpan error", zap.Error(err))
		w.pluginLogger.Error(err.Error())
		return
	}
}

func (w *BatchSpanWriter) uploadRows(ctx context.Context, tableName string, rows []types.Value, metrics *wmetrics.WriteMetrics) error {
	ts := time.Now()
	data := types.ListValue(rows...)

	startTime := time.Now()
	err := w.pool.Do(ctx, func(ctx context.Context, session table.Session) (err error) {
		return session.BulkUpsert(ctx, tableName, data)
	})
	connection_monitor.GlobalConnectionMonitor.LatencyCollector <- time.Since(startTime)

	metrics.Emit(err, time.Since(ts), len(rows))
	return err
}
