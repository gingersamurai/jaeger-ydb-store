package connection_monitor

import (
	"context"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/spf13/viper"
	"github.com/ydb-platform/jaeger-ydb-store/internal/db"
	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"sort"
	"time"
)

type ConnectionMonitor struct {
	discoveryClient  discovery.Client
	pluginLogger     hclog.Logger
	LatencyCollector chan time.Duration
}

var GlobalConnectionMonitor = &ConnectionMonitor{}

func (cm *ConnectionMonitor) Init(ctx context.Context, v *viper.Viper) error {
	conn, err := db.DialFromViper(
		ctx,
		v,
		nil,
		sugar.DSN(v.GetString(db.KeyYdbAddress), v.GetString(db.KeyYdbPath), true),
	)
	if err != nil {
		return fmt.Errorf("newConnectionMonitor(): %w", err)
	}

	discoveryClient := conn.Discovery()
	pluginLogger := hclog.New(&hclog.LoggerOptions{
		Name:       "ydb-store-plugin",
		JSONFormat: true,
	})
	cm.discoveryClient = discoveryClient
	cm.pluginLogger = pluginLogger
	cm.LatencyCollector = make(chan time.Duration, 100)
	return nil
}

func (cm *ConnectionMonitor) RunEndpoints() {
	prevEndpoints := ""
	for {
		endpointsSlice, err := cm.discoveryClient.Discover(context.Background())
		if err != nil {
			cm.pluginLogger.Error(err.Error())
		}
		sort.Slice(endpointsSlice, func(i, j int) bool {
			return endpointsSlice[i].NodeID() < endpointsSlice[j].NodeID()
		})

		endpoints := ""
		for _, ep := range endpointsSlice {
			endpoints += fmt.Sprintf("(%v), ", ep.NodeID())
		}
		if endpoints != prevEndpoints {
			cm.pluginLogger.Warn("ENDPOINTS MONITOR:",
				"endpoints", endpoints,
			)
			prevEndpoints = endpoints
		}
	}
}

func (cm *ConnectionMonitor) RunLatency() {
	maxLatency := time.Duration(0)
	for latency := range cm.LatencyCollector {
		if latency > maxLatency {
			maxLatency = latency
		}
		cm.pluginLogger.Warn("LATENCY MONITOR:",
			"latency", latency.String(),
			"maxLatency", maxLatency.String(),
		)
	}
}
