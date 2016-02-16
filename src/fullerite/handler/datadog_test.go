package handler

import (
	"fullerite/metric"

	"testing"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func getTestDataDogHandler(interval, buffsize, timeoutsec int) *Datadog {
	testChannel := make(chan metric.Metric)
	testLog := l.WithField("testing", "datadog_handler")
	timeout := time.Duration(timeoutsec) * time.Second

	return NewDatadog(testChannel, interval, buffsize, timeout, testLog).(*Datadog)
}

func TestDatadogConfigureEmptyConfig(t *testing.T) {
	config := map[string]interface{}{}

	d := getTestDataDogHandler(12, 13, 14)
	d.Configure(config)

	assert.Equal(t, 12, d.Interval())
	assert.Equal(t, 13, d.MaxBufferSize())
}

func TestDatadogConfigure(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"timeout":         "10",
		"max_buffer_size": "100",
		"endpoint":        "datadog.server",
	}

	d := getTestDataDogHandler(12, 13, 14)
	d.Configure(config)

	assert.Equal(t, 10, d.Interval())
	assert.Equal(t, 100, d.MaxBufferSize())
	assert.Equal(t, "datadog.server", d.Endpoint())
}
