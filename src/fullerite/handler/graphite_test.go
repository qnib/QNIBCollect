package handler

import (
	"fullerite/metric"

	"testing"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func getTestGraphiteHandler(interval, buffsize, timeoutsec int) *Graphite {
	testChannel := make(chan metric.Metric)
	testLog := l.WithField("testing", "graphite_handler")
	timeout := time.Duration(timeoutsec) * time.Second

	return NewGraphite(testChannel, interval, buffsize, timeout, testLog).(*Graphite)
}

func TestGraphiteConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})
	g := getTestGraphiteHandler(12, 13, 14)
	g.Configure(config)

	assert.Equal(t, 12, g.Interval())
}

func TestGraphiteConfigure(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"timeout":         "10",
		"max_buffer_size": "100",
		"server":          "test_server",
		"port":            "10101",
	}

	g := getTestGraphiteHandler(12, 13, 14)
	g.Configure(config)

	assert.Equal(t, 10, g.Interval())
	assert.Equal(t, 100, g.MaxBufferSize())
	assert.Equal(t, "test_server", g.Server())
	assert.Equal(t, "10101", g.Port())
}

func TestGraphiteConfigureIntPort(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"timeout":         "10",
		"max_buffer_size": "100",
		"server":          "test_server",
		"port":            10101,
	}

	g := getTestGraphiteHandler(12, 13, 14)
	g.Configure(config)

	assert.Equal(t, 10, g.Interval())
	assert.Equal(t, 100, g.MaxBufferSize())
	assert.Equal(t, "test_server", g.Server())
	assert.Equal(t, "10101", g.Port())
}
