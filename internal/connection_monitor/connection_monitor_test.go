package connection_monitor

import (
	"context"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConnectionMonitor_Init(t *testing.T) {
	err := GlobalConnectionMonitor.Init(context.Background(), viper.GetViper())
	assert.NoError(t, err)
	//var _ = hclog.New(&hclog.LoggerOptions{
	//	Name:       "ydb-store-plugin",
	//	JSONFormat: true,
	//})

}
