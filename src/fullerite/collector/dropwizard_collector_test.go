package collector

import (
	"fullerite/metric"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func extractMetricWithName(metrics []metric.Metric,
	metricName string) (metric.Metric, bool) {
	var m metric.Metric

	for _, metric := range metrics {
		if metric.Name == metricName {
			return metric, true
		}
	}

	return m, false
}

func extractMetricWithType(metrics []metric.Metric,
	metricType string) (metric.Metric, bool) {
	var m metric.Metric

	for _, metric := range metrics {
		if metric.MetricType == metricType {
			return metric, true
		}
	}

	return m, false
}

func TestDropwizardNtesting(t *testing.T) {
	rawData := []byte(`
{
  "jetty": {
     "percent": {
         "foo": {
            "active-requests": {
              "count": 0,
              "type": "counter"
            }
         }
     }
   }
}
        `)

	metrics, err := parseUWSGIMetrics(&rawData)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(metrics))
}

func TestInvalidMetricSkilled(t *testing.T) {
	rawData := []byte(`
{
        "meters": {
            "pyramid_uwsgi_metrics.tweens.2xx-responses": {
                "units": "events/second"
            }
        }
}
        `)

	metrics, err := parseUWSGIMetrics(&rawData)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(metrics))
}

func TestDropJVMMetrics(t *testing.T) {
	rawData := []byte(`
{
  "jvm": {
    "current_time": 1447699597200,
    "uptime": 419631,
    "thread_count": 315,
    "vm": {
      "version": "1.8.0_45-b14",
      "name": "Java HotSpot(TM) 64-Bit Server VM"
    },
    "garbage-collectors": {
      "ConcurrentMarkSweep": {
        "runs": 13,
        "time": 1531
      },
      "ParNew": {
        "runs": 45146,
        "time": 1324093
      }
    },
    "daemon_thread_count": 96,
    "thread-states": {
      "terminated": 0,
      "runnable": 0.17777777777777778,
      "timed_waiting": 0.7714285714285715,
      "waiting": 0.050793650793650794,
      "new": 0,
      "blocked": 0
    }
  }
}
        `)

	metrics, err := parseUWSGIMetrics(&rawData)
	assert.Nil(t, err)
	assert.Equal(t, 14, len(metrics))
}

func TestTimerMetrics(t *testing.T) {
	rawData := []byte(`
{
  "jetty": {
    "trace-requests": {
      "duration": {
        "p98": 0,
        "p99": 0,
        "unit": "milliseconds",
        "mean": 0
      },
      "rate": {
        "count": 0,
        "m5": 0,
        "m15": 0,
        "m1": 0,
        "unit": "seconds",
        "mean": 0
      },
      "type": "timer"
    },
    "foo": {
      "type": "gauge",
      "value": 5.612
    }
  }
}
        `)
	metrics, err := parseUWSGIMetrics(&rawData)
	assert.Nil(t, err)
	assert.Equal(t, 9, len(metrics))
	t.Log(metrics)
}

func TestGauge(t *testing.T) {
	rawData := []byte(`
{
  "org.eclipse.jetty.servlet.ServletContextHandler": {
    "percent-4xx-1m": {
      "type": "gauge",
      "value": 5.611051195902441e-77
    }
  }
}
        `)
	metrics, err := parseUWSGIMetrics(&rawData)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(metrics))
}

func TestBigFileJSON(t *testing.T) {
	dat, err := ioutil.ReadFile("sample.json")

	assert.Nil(t, err)

	metrics, err := parseUWSGIMetrics(&dat)
	assert.Nil(t, err)
	assert.Equal(t, 560, len(metrics))
}

func TestHistogram(t *testing.T) {
	rawData := []byte(`
{
  "foo": {
    "bar": {
      "type": "histogram",
      "count": 100,
      "min": 2,
      "max": 2,
      "mean": 2,
      "std_dev": 0,
      "median": 2,
      "p75": 2,
      "p95": 2,
      "p98": 2,
      "p99": 2,
      "p999": 2
    }
  }
}
        `)
	metrics, err := parseUWSGIMetrics(&rawData)
	assert.Nil(t, err)
	assert.Equal(t, 11, len(metrics))
	t.Log(len(metrics))
	counterMetric, ok := extractMetricWithType(metrics, "COUNTER")
	assert.True(t, ok)

	assert.Equal(t, 100.0, counterMetric.Value)
}
