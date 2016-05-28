package handler

import (
	"fmt"
	"fullerite/metric"
	"testing"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func getTestZmqBUFHandler(interval, buffsize, timeoutsec int) *ZmqBUF {
	testChannel := make(chan metric.Metric)
	testLog := l.WithField("testing", "zmqbuf_handler")
	timeout := time.Duration(timeoutsec) * time.Second

	return newZmqBUF(testChannel, interval, buffsize, timeout, testLog).(*ZmqBUF)
}

func TestZmqBUFConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})

	h := getTestZmqBUFHandler(12, 13, 14)
	h.Configure(config)

	assert.Equal(t, 12, h.Interval())
}

func TestZmqBUFConfigure(t *testing.T) {
	config := map[string]interface{}{
		"port":      "6060",
		"retention": "300000",
	}

	h := getTestZmqBUFHandler(12, 13, 14)
	h.Configure(config)
	assert.Equal(t, "6060", h.Port())
	assert.Equal(t, 300000, h.Retention())
}

func TestZmqBUFValues(t *testing.T) {
	config := map[string]interface{}{
		"port":      "6060",
		"retention": "300000",
	}

	h := getTestZmqBUFHandler(12, 13, 14)
	h.Configure(config)
	var m metric.Metric
	exp := []metric.Metric{}
	_, ok := h.Values()
	assert.False(t, ok, "Values() returned non-empty list")
	for i := 0; i < 5; i++ {
		m = metric.New(fmt.Sprintf("m%d", i))
		m.EnableBuffering()
		exp = append(exp, m)
		h.Enqueue(m)
		time.Sleep(100 * time.Millisecond)
	}
	slice, ok := h.Values()
	assert.True(t, ok, "Values() returned empty list")
	assert.Equal(t, exp, slice)
}

func TestZmqBUFFilter(t *testing.T) {
	config := map[string]interface{}{
		"port":      "6060",
		"retention": "300000",
	}

	h := getTestZmqBUFHandler(12, 13, 14)
	h.Configure(config)
	var m metric.Metric
	_, ok := h.Values()
	assert.False(t, ok, "Values() returned non-empty list")
	for i := 0; i < 5; i++ {
		m = metric.New(fmt.Sprintf("m%d", i))
		m.EnableBuffering()
		h.Enqueue(m)
		time.Sleep(100 * time.Millisecond)
	}
	d := map[string]string{}
	f := metric.NewFilter("m.*", "gauge", d)
	slice, ok := h.Filter(f)
	assert.True(t, ok, "Values() returned empty list")
	assert.Equal(t, []metric.Metric{}, slice)
	f = metric.NewFilter("m0", "gauge", d)
	slice, ok = h.Match(f)
	assert.True(t, ok, "Values() returned empty list")
	assert.Equal(t, 1, len(slice))
}
