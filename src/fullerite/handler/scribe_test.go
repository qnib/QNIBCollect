package handler

import (
	"fullerite/metric"

	"regexp"
	"testing"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/samuel/go-thrift/examples/scribe"
	"github.com/stretchr/testify/assert"
)

func getTestScribeHandler(interval, buffsize, timeoutsec int) *Scribe {
	testChannel := make(chan metric.Metric)
	testLog := l.WithField("testing", "scribe_handler")
	timeout := time.Duration(timeoutsec) * time.Second

	return NewScribe(testChannel, interval, buffsize, timeout, testLog).(*Scribe)
}

type MockScribeClient struct {
	msg []*scribe.LogEntry
}

func (m *MockScribeClient) Log(Messages []*scribe.LogEntry) (scribe.ResultCode, error) {
	m.msg = Messages
	return scribe.ResultCodeByName["ResultCode.OK"], nil
}

func TestScribeConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})

	s := getTestScribeHandler(12, 13, 14)
	s.Configure(config)

	assert.Equal(t, 12, s.Interval())
	assert.Equal(t, 13, s.MaxBufferSize())
	assert.Equal(t, defaultScribeEndpoint, s.endpoint)
	assert.Equal(t, defaultScribePort, s.port)
	assert.Equal(t, defaultScribeStreamName, s.streamName)
	assert.Nil(t, s.scribeClient)
}

func TestScribeConfigure(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"timeout":         "10",
		"max_buffer_size": "100",
		"endpoint":        "1.2.3.4",
		"port":            123,
		"streamName":      "my_stream",
	}

	s := getTestScribeHandler(40, 50, 60)
	s.Configure(config)

	assert.Equal(t, 10, s.Interval())
	assert.Equal(t, 100, s.MaxBufferSize())
	assert.Equal(t, "1.2.3.4", s.endpoint)
	assert.Equal(t, 123, s.port)
	assert.Equal(t, "my_stream", s.streamName)
	assert.Nil(t, s.scribeClient)
}

func TestScribeEmitMetricsNoClient(t *testing.T) {
	s := getTestScribeHandler(40, 50, 60)

	m := metric.Metric{}
	res := s.emitMetrics([]metric.Metric{m})
	assert.False(t, res, "Should not emit metrics if the scribeClient is nil")
}

func TestScribeEmitMetricsZeroMetrics(t *testing.T) {
	s := getTestScribeHandler(40, 50, 60)
	s.scribeClient = &MockScribeClient{}

	res := s.emitMetrics([]metric.Metric{})
	assert.False(t, res, "Should not emit anything if there are not metrics")
}

func TestScribeEmitMetrics(t *testing.T) {
	s := getTestScribeHandler(40, 50, 60)
	m := &MockScribeClient{}
	s.streamName = "my_stream"
	s.scribeClient = m

	metrics := []metric.Metric{
		metric.Metric{
			Name:       "test1",
			MetricType: metric.Gauge,
			Value:      1,
			Dimensions: map[string]string{"dim1": "val1"},
		},
		metric.Metric{
			Name:       "test2",
			Value:      2,
			MetricType: metric.Counter,
			Dimensions: map[string]string{"dim2": "val2"},
		},
	}

	res := s.emitMetrics(metrics)
	assert.True(t, res)

	assert.Equal(t, "my_stream", m.msg[0].Category)
	matched, _ := regexp.MatchString(
		"{\"name\":\"test1\",\"type\":\"gauge\",\"value\":1,\"timestamp\":\\d*,\"dimensions\":{\"dim1\":\"val1\"}}",
		m.msg[0].Message,
	)
	assert.True(t, matched)

	assert.Equal(t, "my_stream", m.msg[1].Category)
	matched, _ = regexp.MatchString(
		"{\"name\":\"test2\",\"type\":\"counter\",\"value\":2,\"timestamp\":\\d*,\"dimensions\":{\"dim2\":\"val2\"}}",
		m.msg[1].Message,
	)
	assert.True(t, matched)
}

func TestCreateScribeMetricsWithDefaultDimensions(t *testing.T) {
	s := getTestScribeHandler(40, 50, 60)
	config := map[string]interface{}{
		"defaultDimensions": map[string]string{"region": "uswest1-devc", "ecosystem": "devc"},
	}
	s.Configure(config)

	m := metric.Metric{
		Name:       "test1",
		MetricType: metric.Gauge,
		Value:      1,
		Dimensions: map[string]string{"dim1": "val1", "ecosystem": "devb"},
	}

	res := s.createScribeMetric(m)
	assert.Equal(t, map[string]string{"region": "uswest1-devc", "ecosystem": "devc", "dim1": "val1"}, res.Dimensions)
}
