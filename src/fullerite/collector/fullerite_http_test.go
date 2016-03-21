package collector

import (
	"fullerite/metric"

	"bytes"
	"io"
	"net/http"
	"testing"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func getTestInstance() *fulleriteHTTP {
	testChannel := make(chan metric.Metric)
	testLog = l.WithFields(l.Fields{"testing": "fullerite_http"})

	inst := newFulleriteHTTP(testChannel, 12, testLog).(*fulleriteHTTP)

	return inst
}

func getTestResponse() string {
	testString := `
	{
		"memory": {
			"counters": {
				"somemem": 23
			}, "gauges": {
				"somememgauge": 342.2
			}
		}, "handlers": {
			"firsthandler": {
				"counters": {
					"firstcounter": 213
				}, "gauges": {
					"firstgauge": 123
				}
			}, "secondhandler": { 
				"counters": {
					"secondcounter": 234,
					"secondsecondcounter": 53.2
				}, "gauges": {
					"secondgauge": 245.3
				}
			}
		}, "collectors": {
			"firstcollector": {
				"counters": {
					"metric_emission": 314
				}, "gauges": {
					"last_count": 111
				}
			}
                }
	}
	`
	return testString
}

type noopCloser struct {
	io.Reader
}

func (n noopCloser) Close() error { return nil }

func buildTestHTTPResponse(body string) *http.Response {
	rsp := new(http.Response)

	rsp.Body = noopCloser{bytes.NewBufferString(body)}

	return rsp
}

func TestMakeNewFulleriteHTTP(t *testing.T) {
	inst := getTestInstance()
	assert.NotNil(t, inst)

	assert.Equal(t, "http://localhost:9090/metrics", inst.endpoint)
	assert.NotNil(t, inst.errHandler)
	assert.NotNil(t, inst.rspHandler)
}

func TestConfigureFulleriteHTTP(t *testing.T) {
	config := make(map[string]interface{})
	config["endpoint"] = "http://somewhere:234/marp"
	config["interval"] = 123

	inst := getTestInstance()
	inst.Configure(config)

	assert.Equal(t, "http://somewhere:234/marp", inst.endpoint)
	assert.Equal(t, 123, inst.Interval())
}

func TestHandleBadInputFulleriteHTTP(t *testing.T) {
	rsp := buildTestHTTPResponse("teststring")

	inst := getTestInstance()
	results := inst.rspHandler(rsp)

	assert.NotNil(t, results)
	assert.Equal(t, 0, len(results))
}

func TestHandleNotJson(t *testing.T) {
	txt := []byte("not json")
	inst := getTestInstance()
	metrics, err := inst.parseResponseText(&txt)

	assert.NotNil(t, metrics)
	assert.NotNil(t, err)
	assert.Equal(t, 0, len(metrics))
}

func TestHandlePopulatedResponseFulleriteHTTP(t *testing.T) {
	asBytes := []byte(getTestResponse())

	inst := getTestInstance()
	metrics, err := inst.parseResponseText(&asBytes)

	assert.Nil(t, err)
	assert.Equal(t, 9, len(metrics))

	assertDimension := func(m *metric.Metric, key, val string) {
		actual, exists := m.GetDimensionValue(key)
		assert.True(t, exists)
		assert.Equal(t, val, actual)
	}

	for _, m := range metrics {

		switch m.Name {
		case "somemem":
			assert.Equal(t, 23.0, m.Value)
			assert.Equal(t, metric.CumulativeCounter, m.MetricType)
		case "somememgauge":
			assert.Equal(t, 342.2, m.Value)
		case "firstcounter":
			assert.Equal(t, 213.0, m.Value)
			assert.Equal(t, metric.CumulativeCounter, m.MetricType)
			assertDimension(&m, "handler", "firsthandler")
		case "firstgauge":
			assert.Equal(t, 123.0, m.Value)
			assertDimension(&m, "handler", "firsthandler")
		case "secondcounter":
			assert.Equal(t, 234.0, m.Value)
			assertDimension(&m, "handler", "secondhandler")
			assert.Equal(t, metric.CumulativeCounter, m.MetricType)
		case "secondsecondcounter":
			assert.Equal(t, 53.2, m.Value)
			assertDimension(&m, "handler", "secondhandler")
			assert.Equal(t, metric.CumulativeCounter, m.MetricType)
		case "secondgauge":
			assert.Equal(t, 245.3, m.Value)
			assertDimension(&m, "handler", "secondhandler")
		case "metric_emission":
			assert.Equal(t, 314.0, m.Value)
			assertDimension(&m, "collector", "firstcollector")
		}
	}
}
