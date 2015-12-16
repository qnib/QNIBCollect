package handler

import (
	"fullerite/metric"

	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func getTestLogHandler(interval int, buffsize int, bufferflushinterval int) *Log {
	testChannel := make(chan metric.Metric)
	testLog := l.WithField("testing", "log_handler")
	flush := time.Duration(bufferflushinterval) * time.Second

	return NewLog(testChannel, interval, buffsize, flush, testLog)
}

func TestLogConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})
	h := getTestLogHandler(12, 13, 1)
	h.Configure(config)

	assert.Equal(t, 12, h.Interval())
	assert.Equal(t, 13, h.MaxBufferSize())
}

func TestLogConfigure(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"max_buffer_size": "100",
	}

	h := getTestLogHandler(12, 13, 1)
	h.Configure(config)

	assert.Equal(t, 10, h.Interval())
	assert.Equal(t, 100, h.MaxBufferSize())
}

func TestConvertToLog(t *testing.T) {

	h := getTestLogHandler(12, 13, 1)
	m := metric.New("TestMetric")

	dpString, err := h.convertToLog(m)
	if err != nil {
		t.Errorf("convertToLog failed to convert %q: err", m, err)
	}

	assert.Equal(t, "{\"name\":\"TestMetric\",\"type\":\"gauge\",\"value\":0,\"dimensions\":{}}", dpString)
}
