package collector

import (
	"fullerite/metric"

	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func getTestNerveConfig() []byte {
	raw := `
	{
	    "heartbeat_path": "/var/run/nerve/heartbeat",
	    "instance_id": "srv1-devc",
	    "services": {
	        "example_service.main.norcal-devc.superregion:norcal-devc.13752.new": {
	            "check_interval": 7,
	            "checks": [
	                {
	                    "fall": 2,
	                    "headers": {},
	                    "host": "127.0.0.1",
	                    "open_timeout": 6,
	                    "port": 6666,
	                    "rise": 1,
	                    "timeout": 6,
	                    "type": "http",
	                    "uri": "/http/example_service.main/13752/status"
	                }
	            ],
	            "host": "10.56.5.21",
	            "port": 13752,
	            "weight": 24,
	            "zk_hosts": [
	                "10.40.5.5:22181",
	                "10.40.5.6:22181",
	                "10.40.1.17:22181"
	            ],
	            "zk_path": "/nerve/superregion:norcal-devc/example_service.main"
	        },
	        "example_service.mesosstage_main.norcal-devc.superregion:norcal-devc.13752.new": {
	            "check_interval": 7,
	            "checks": [
	                {
	                    "fall": 2,
	                    "headers": {},
	                    "host": "127.0.0.1",
	                    "open_timeout": 6,
	                    "port": 6666,
	                    "rise": 1,
	                    "timeout": 6,
	                    "type": "http",
	                    "uri": "/http/example_service.mesosstage_main/13752/status"
	                }
	            ],
	            "host": "10.56.5.21",
	            "port": 22222,
	            "weight": 24,
	            "zk_hosts": [
	                "10.40.5.5:22181",
	                "10.40.5.6:22181",
	                "10.40.1.17:22181"
	            ],
	            "zk_path": "/nerve/superregion:norcal-devc/example_service.mesosstage_main"
	        }
	    }
	}
	`
	return []byte(raw)
}

func getTestUWSGIResponse() string {
	return `{
	"counters": {
		"Acounter":{
			"firstrollup": 134,
			"secondrollup": 89
		}
	},
	"meters": {},
	"timers": {
		"some_timer": {
			"average": 123
		},
		"othertimer": {
			"mean": 345
		}
	},
	"gauges": {
		"some_random_metric": {
			"rollup1": 12
		}
	},
	"histograms": {}
	}
	`
}

func validateUWSGIResults(t *testing.T, actual []metric.Metric) {
	assert.Equal(t, 5, len(actual))

	for _, m := range actual {
		metricTypeDim, exists := m.GetDimensionValue("type")
		assert.True(t, exists)
		rollup, exists := m.GetDimensionValue("rollup")
		assert.True(t, exists)

		switch m.Name {
		case "some_random_metric":
			assert.Equal(t, "rollup1", rollup)
			assert.Equal(t, "gauge", metricTypeDim)
			assert.Equal(t, 12.0, m.Value)
			assert.Equal(t, metric.Gauge, m.MetricType)
		case "othertimer":
			assert.Equal(t, "mean", rollup)
			assert.Equal(t, "timer", metricTypeDim)
			assert.Equal(t, 345.0, m.Value)
			assert.Equal(t, metric.Gauge, m.MetricType)
		case "some_timer":
			assert.Equal(t, "average", rollup)
			assert.Equal(t, "timer", metricTypeDim)
			assert.Equal(t, 123.0, m.Value)
			assert.Equal(t, metric.Gauge, m.MetricType)
		case "ACounter":
			assert.Equal(t, metric.Counter, m.MetricType)
			val, exists := map[string]float64{
				"firstrollup":  134,
				"secondrollup": 89,
			}[rollup]
			assert.Equal(t, "counter", metricTypeDim)
			assert.True(t, exists)
			assert.Equal(t, val, m.Value)
		}
	}
}

func validateFullDimensions(t *testing.T, actual []metric.Metric, serviceName, port string) {
	for _, m := range actual {
		assert.Equal(t, 5, len(m.Dimensions))

		val, exists := m.GetDimensionValue("collector")
		assert.True(t, exists)
		assert.Equal(t, "NerveUWSGI", val)

		val, exists = m.GetDimensionValue("service")
		assert.True(t, exists)
		assert.Equal(t, serviceName, val)

		val, exists = m.GetDimensionValue("port")
		assert.True(t, exists)
		assert.Equal(t, port, val)
	}

}

func validateEmptyChannel(t *testing.T, c chan metric.Metric) {
	close(c)
	for m := range c {
		t.Fatalf("The channel was not empty! got value ", m)
	}
}

func parseURL(url string) (string, string) {
	parts := strings.Split(url, ":")
	ip := strings.Replace(parts[1], "/", "", -1)
	port := parts[2]
	return ip, port
}

func getTestNerveUWSGI() *nerveUWSGICollector {
	return newNerveUWSGICollector(make(chan metric.Metric), 12, l.WithField("testing", "nerveuwsgi"))
}

func TestNerveConfigParsing(t *testing.T) {
	expected := map[int]string{
		22222: "example_service",
		13752: "example_service",
	}

	cfgString := getTestNerveConfig()
	results, err := getTestNerveUWSGI().parseNerveConfig(&cfgString, []string{"10.56.5.21"})
	assert.Nil(t, err)
	assert.Equal(t, expected, results)
}

func TestNerveFilterOnIP(t *testing.T) {
	cfgString := getTestNerveConfig()
	results, err := getTestNerveUWSGI().parseNerveConfig(&cfgString, []string{"10.56.2.3"})
	assert.Nil(t, err)
	assert.NotNil(t, results)
	assert.Equal(t, 0, len(results))
}

func TestHandleBadNerveConfig(t *testing.T) {
	// b/c there is valid json coming in it won't error, just have an empty response
	cfgString := []byte("{}")
	results, err := getTestNerveUWSGI().parseNerveConfig(&cfgString, []string{"10.56.2.3"})
	assert.Nil(t, err)
	assert.NotNil(t, results)
	assert.Equal(t, 0, len(results))
}

func TestHandlePoorlyFormedJson(t *testing.T) {
	cfgString := []byte("notjson")
	results, err := getTestNerveUWSGI().parseNerveConfig(&cfgString, []string{"10.56.2.3"})
	assert.NotNil(t, err)
	assert.NotNil(t, results)
	assert.Equal(t, 0, len(results))
}

func TestDefaultConfigNerveUWSGI(t *testing.T) {
	inst := getTestNerveUWSGI()
	inst.Configure(make(map[string]interface{}))

	assert.Equal(t, 12, inst.Interval())
	assert.Equal(t, "/etc/nerve/nerve.conf.json", inst.configFilePath)
	assert.Equal(t, "status/metrics", inst.queryPath)
}

func TestConfigNerveUWSGI(t *testing.T) {
	cfg := map[string]interface{}{
		"interval":       345,
		"configFilePath": "/etc/your/moms/house",
		"queryPath":      "littlepiggies",
	}

	inst := getTestNerveUWSGI()
	inst.Configure(cfg)

	assert.Equal(t, 345, inst.Interval())
	assert.Equal(t, "/etc/your/moms/house", inst.configFilePath)
	assert.Equal(t, "littlepiggies", inst.queryPath)
}

func TestUWSGIMetricConversion(t *testing.T) {
	testMeters := make(map[string]map[string]interface{})
	testMeters["pyramid_uwsgi_metrics.tweens.5xx-responses"] = map[string]interface{}{
		"count":     957,
		"mean_rate": 0.0006172935981330262,
		"m15_rate":  2.8984757611832113e-41,
		"m5_rate":   1.8870959302511822e-119,
		"m1_rate":   3e-323,

		// this will not create a metric
		"units": "events/second",
	}
	testMeters["pyramid_uwsgi_metrics.tweens.4xx-responses"] = map[string]interface{}{
		"count":     366116,
		"mean_rate": 0.2333071157843687,
		"m15_rate":  0.22693345170298124,
		"m5_rate":   0.21433439128223822,
		"m1_rate":   0.14771304656654516,

		// this will not create a metric
		"units": "events/second",
	}

	actual := convertToMetrics(&testMeters, "metricType")

	// only the numbers are made
	assert.Equal(t, 10, len(actual))
	for _, m := range actual {
		assert.Equal(t, "metricType", m.MetricType)

		// the other dims are applied at a higher level
		assert.Equal(t, 1, len(m.Dimensions))

		rollup, exists := m.GetDimensionValue("rollup")
		assert.True(t, exists)

		switch m.Name {
		case "pyramid_uwsgi_metrics.tweens.5xx-responses":
			val, exists := map[string]float64{
				"mean_rate": 0.0006172935981330262,
				"m15_rate":  2.8984757611832113e-41,
				"m5_rate":   1.8870959302511822e-119,
				"m1_rate":   3e-323,
				"count":     957,
			}[rollup]
			assert.True(t, exists, "unknown rollup "+rollup)
			assert.Equal(t, val, m.Value)
		case "pyramid_uwsgi_metrics.tweens.4xx-responses":
			val, exists := map[string]float64{
				"count":     366116,
				"mean_rate": 0.2333071157843687,
				"m15_rate":  0.22693345170298124,
				"m5_rate":   0.21433439128223822,
				"m1_rate":   0.14771304656654516,
			}[rollup]
			assert.True(t, exists, "unknown rollup "+rollup)
			assert.Equal(t, val, m.Value, "mismatching value on rollup "+rollup)
		default:
			t.Fatalf("unknown metric name %s", m.Name)
		}
	}
}

func TestJavaUWSGIResponseParsing(t *testing.T) {

}

func TestUWSGIResponseConversion(t *testing.T) {
	uwsgiRsp := []byte(getTestUWSGIResponse())

	actual, err := parseUWSGIMetrics(&uwsgiRsp)
	assert.Nil(t, err)
	validateUWSGIResults(t, actual)
	for _, m := range actual {
		assert.Equal(t, 2, len(m.Dimensions))
	}
}

func TestNerveUWSGICollect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rsp *http.Request) {
		// fmt.Println("MARRRRP")
		fmt.Fprint(w, getTestUWSGIResponse())
	}))
	defer server.Close()

	// assume format is http://ipaddr:port
	ip, port := parseURL(server.URL)

	minimalNerveConfig := make(map[string]map[string]map[string]interface{})
	minimalNerveConfig["services"] = map[string]map[string]interface{}{
		"test_service.things.and.stuff": {
			"host": ip,
			"port": port,
		},
	}

	tmpFile, err := ioutil.TempFile("", "fullerite_testing")
	defer os.Remove(tmpFile.Name())
	assert.Nil(t, err)

	marshalled, err := json.Marshal(minimalNerveConfig)
	assert.Nil(t, err)

	_, err = tmpFile.Write(marshalled)
	assert.Nil(t, err)

	cfg := map[string]interface{}{
		"configFilePath": tmpFile.Name(),
		"queryPath":      "",
	}

	inst := getTestNerveUWSGI()
	inst.Configure(cfg)

	go inst.Collect()

	actual := []metric.Metric{}
	for i := 0; i < 5; i++ {
		actual = append(actual, <-inst.Channel())
	}

	validateUWSGIResults(t, actual)
	validateFullDimensions(t, actual, "test_service", port)
	validateEmptyChannel(t, inst.Channel())
}

func TestNonConflictingServiceQueries(t *testing.T) {
	goodServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rsp *http.Request) {
		fmt.Fprint(w, getTestUWSGIResponse())
	}))
	defer goodServer.Close()

	badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rsp *http.Request) {
		return // no response
	}))
	defer badServer.Close()

	goodIP, goodPort := parseURL(goodServer.URL)
	badIP, badPort := parseURL(badServer.URL)

	minimalNerveConfig := make(map[string]map[string]map[string]interface{})
	minimalNerveConfig["services"] = map[string]map[string]interface{}{
		"test_service.things.and.stuff": {
			"host": goodIP,
			"port": goodPort,
		},
		"other_service.does.lots.of.stuff": {
			"host": badIP,
			"port": badPort,
		},
	}

	tmpFile, err := ioutil.TempFile("", "fullerite_testing")
	defer os.Remove(tmpFile.Name())
	assert.Nil(t, err)

	marshalled, err := json.Marshal(minimalNerveConfig)
	assert.Nil(t, err)

	_, err = tmpFile.Write(marshalled)
	assert.Nil(t, err)

	cfg := map[string]interface{}{
		"configFilePath": tmpFile.Name(),
		"queryPath":      "",
	}

	inst := getTestNerveUWSGI()
	inst.Configure(cfg)

	go inst.Collect()

	actual := []metric.Metric{}
	for i := 0; i < 5; i++ {
		actual = append(actual, <-inst.Channel())
	}

	validateUWSGIResults(t, actual)
	validateFullDimensions(t, actual, "test_service", goodPort)
	validateEmptyChannel(t, inst.Channel())
}
