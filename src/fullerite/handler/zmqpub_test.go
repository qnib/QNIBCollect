package handler

import (
	"fullerite/metric"
	"testing"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func getTestZmqPUBHandler(interval, buffsize, timeoutsec int) *ZmqPUB {
	testChannel := make(chan metric.Metric)
	testLog := l.WithField("testing", "zmqpub_handler")
	timeout := time.Duration(timeoutsec) * time.Second

	return newZmqPUB(testChannel, interval, buffsize, timeout, testLog).(*ZmqPUB)
}

func TestZmqPUBConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})

	h := getTestZmqPUBHandler(12, 13, 14)
	h.Configure(config)

	assert.Equal(t, 12, h.Interval())
}

func TestZmqPUBConfigure(t *testing.T) {
	config := map[string]interface{}{
		"port": "5555",
	}

	h := getTestZmqPUBHandler(12, 13, 14)
	h.Configure(config)
	assert.Equal(t, "5555", h.Port())
}

func TestZmqPUBConfigureIntPort(t *testing.T) {
	config := map[string]interface{}{
		"port": "5555",
	}

	h := getTestZmqPUBHandler(12, 13, 14)
	h.Configure(config)

	assert.Equal(t, "5555", h.Port())
}
