package main

import (
	"fullerite/collector"
	"fullerite/metric"
	"test_utils"

	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCollectorLogsErrors(t *testing.T) {
	testLogger := test_utils.BuildLogger()
	testLogger = testLogger.WithField("collector", "Test")

	channel := make(chan metric.Metric)
	config := make(map[string]interface{})

	testCol := collector.NewTest(channel, 123, testLogger)
	testCol.Configure(config)

	hook := NewLogErrorHook(testCol.Channel())
	testLogger.Logger.Hooks.Add(hook)

	go testCol.Collect()
	testLogger.Error("testing Error log")

	select {
	case m := <-testCol.Channel():
		assert.Equal(t, "fullerite.collector_errors", m.Name)
		assert.Equal(t, 1.0, m.Value)
		assert.Equal(t, "Test", m.Dimensions["collector"])
		return
	case <-time.After(1 * time.Second):
		t.Fail()
	}
}
