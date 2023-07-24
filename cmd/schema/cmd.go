package main

import (
	"context"
	"fmt"
	stdLog "log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ydb-platform/jaeger-ydb-store/internal/log"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/ydb-platform/jaeger-ydb-store/cmd/schema/watcher"
	"github.com/ydb-platform/jaeger-ydb-store/internal/db"
	localViper "github.com/ydb-platform/jaeger-ydb-store/internal/viper"
	"github.com/ydb-platform/jaeger-ydb-store/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func init() {
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	viper.SetDefault("watcher_interval", time.Minute*5)
	viper.SetDefault("watcher_age", time.Hour*24)
	viper.SetDefault("watcher_lookahead", time.Hour*12)
	viper.SetDefault("parts_traces", 32)
	viper.SetDefault("parts_idx_tag", 32)
	viper.SetDefault("parts_idx_tag_v2", 32)
	viper.SetDefault("parts_idx_duration", 32)
	viper.SetDefault("parts_idx_service_name", 32)
	viper.SetDefault("parts_idx_service_op", 32)
	viper.SetDefault(db.KeyYDBPartitionSize, "1024mb")
	viper.AutomaticEnv()
}

func main() {
	localViper.ConfigureViperFromFlag(viper.GetViper())

	command := &cobra.Command{
		Use: "jaeger-ydb-schema",
	}
	command.PersistentFlags().String("address", "", "ydb host:port (env: YDB_ADDRESS)")
	command.PersistentFlags().String("path", "", "ydb path (env: YDB_PATH)")
	command.PersistentFlags().String("folder", "", "ydb folder (env: YDB_FOLDER)")
	command.PersistentFlags().String("token", "", "ydb oauth token (env: YDB_TOKEN)")
	command.PersistentFlags().String("config", "", "path to config file to configure Viper from")

	viper.BindPFlag("ydb_address", command.PersistentFlags().Lookup("address"))
	viper.BindPFlag("ydb_path", command.PersistentFlags().Lookup("path"))
	viper.BindPFlag("ydb_folder", command.PersistentFlags().Lookup("folder"))
	viper.BindPFlag("ydb_token", command.PersistentFlags().Lookup("token"))

	logger, err := log.NewLogger(viper.GetViper())
	if err != nil {
		stdLog.Fatal(err)
	}

	watcherCmd := &cobra.Command{
		Use: "watcher",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := watcher.Options{
				Expiration: viper.GetDuration("watcher_age"),
				Lookahead:  viper.GetDuration("watcher_lookahead"),
				DBPath: schema.DbPath{
					Path:   viper.GetString(db.KeyYdbPath),
					Folder: viper.GetString(db.KeyYdbFolder),
				},
			}
			if opts.Expiration == 0 {
				return fmt.Errorf("cannot use watcher age '%s'", opts.Expiration)
			}

			shutdown := make(chan os.Signal, 1)
			signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
			conn, err := ydbConn(viper.GetViper())
			if err != nil {
				return fmt.Errorf("failed to create table client: %w", err)
			}

			logger.Info("starting watcher")
			w := watcher.NewWatcher(opts, conn.Table(), logger)
			w.Run(viper.GetDuration("watcher_interval"))
			<-shutdown
			logger.Info("stopping watcher")
			return nil
		},
	}
	dropCmd := &cobra.Command{
		Use: "drop-tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			dbPath := schema.DbPath{
				Path:   viper.GetString(db.KeyYdbPath),
				Folder: viper.GetString(db.KeyYdbFolder),
			}
			conn, err := ydbConn(viper.GetViper())
			if err != nil {
				return fmt.Errorf("failed to create table client: %w", err)
			}
			d, err := conn.Scheme().ListDirectory(ctx, dbPath.String())
			if err != nil {
				return err
			}
			for _, c := range d.Children {
				if c.Type == scheme.EntryTable {
					opCtx, opCancel := context.WithTimeout(context.Background(), time.Second*5)
					fullName := dbPath.FullTable(c.Name)
					err = conn.Table().Do(opCtx, func(ctx context.Context, session table.Session) error {
						return session.DropTable(ctx, fullName)
					})
					opCancel()
					if err != nil {
						return err
					}
					logger.Warn(
						"dropped table",
						"table", fullName,
					)
				}
			}
			return nil
		},
	}
	command.AddCommand(watcherCmd, dropCmd)

	err = command.Execute()
	if err != nil {
		logger.Error(
			"command execute failed",
			"error", err,
		)
		os.Exit(1)
	}
}

func ydbConn(v *viper.Viper) (*ydb.Driver, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	return db.ConnectToYDB(ctx, v, sugar.DSN(v.GetString(db.KeyYdbAddress), v.GetString(db.KeyYdbPath), false))
}
