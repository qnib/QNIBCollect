package handler

import (
	"fullerite/metric"

	"testing"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/stretchr/testify/assert"
)

func getTestInfluxDBHandler(interval, buffsize, timeoutsec int) *InfluxDB {
	testChannel := make(chan metric.Metric)
	testLog := l.WithField("testing", "influxdb_handler")
	timeout := time.Duration(timeoutsec) * time.Second

	return newInfluxDB(testChannel, interval, buffsize, timeout, testLog).(*InfluxDB)
}

func TestInfluxDBConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})
	h := getTestInfluxDBHandler(12, 13, 14)
	h.Configure(config)

	assert.Equal(t, 12, h.Interval())
}

func TestInfluxDBConfigure(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"timeout":         "10",
		"max_buffer_size": "100",
		"server":          "test_server",
		"port":            "8086",
		"database":        "influxdb",
		"username":        "root",
		"password":        "root",
	}

	i := getTestInfluxDBHandler(12, 13, 14)
	i.Configure(config)

	assert.Equal(t, 10, i.Interval())
	assert.Equal(t, 100, i.MaxBufferSize())
	assert.Equal(t, "test_server", i.Server())
	assert.Equal(t, "8086", i.Port())
}

func TestInfluxDBConfigureIntPort(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"timeout":         "10",
		"max_buffer_size": "100",
		"server":          "test_server",
		"port":            "8086",
	}

	i := getTestInfluxDBHandler(12, 13, 14)
	i.Configure(config)

	assert.Equal(t, 10, i.Interval())
	assert.Equal(t, 100, i.MaxBufferSize())
	assert.Equal(t, "test_server", i.Server())
	assert.Equal(t, "8086", i.Port())
}

// TestConvertToInfluxDB tests the plain handler convertion
func TestConvertToInfluxDB(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"timeout":         "10",
		"max_buffer_size": "100",
		"server":          "test_server",
		"port":            "8086",
	}

	i := getTestInfluxDBHandler(12, 13, 14)
	i.Configure(config)

	now := time.Now()
	m := metric.New("TestMetric")
	m.MetricTime = now
	tags := map[string]string{}
	fields := map[string]interface{}{
		"value": 0.0,
	}
	pt := i.convertToInfluxDB(m)
	exp, _ := client.NewPoint("TestMetric", tags, fields, now)
	assert.Equal(t, pt, exp)
}
