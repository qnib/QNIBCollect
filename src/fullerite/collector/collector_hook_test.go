package collector

import (
	"fullerite/metric"

	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestCollectorLogsErrors(t *testing.T) {
	testLogger := logrus.New()
	channel := make(chan metric.Metric)
	config := make(map[string]interface{})

	testCol := NewTest(channel, 123, testLogger.WithFields(logrus.Fields{"testing": "hooks"}))
	testCol.Configure(config)

	hook := NewLogErrorHook(testCol.Channel())
	testLogger.Hooks.Add(hook)

	go testCol.Collect()
	testLogger.Error("testing Error log")

	select {
	case m := <-testCol.Channel():
		assert.Equal(t, "fullerite.collector_errors", m.Name)
		assert.Equal(t, 1.0, m.Value)
		return
	case <-time.After(1 * time.Second):
		t.Fail()
	}
}
