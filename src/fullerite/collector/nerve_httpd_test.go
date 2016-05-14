package collector

import (
	"encoding/json"
	"fmt"
	"fullerite/metric"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func getNerveHTTPDCollector() *NerveHTTPD {
	return newNerveHTTPD(make(chan metric.Metric), 10, l.WithField("testing", "nervehttpd")).(*NerveHTTPD)
}

func getRawApacheStat() []byte {
	metric := []byte(`
Total Accesses: 99
Total kBytes: 108
CPULoad: 901.485
Uptime: 68
ReqPerSec: 1.45588
BytesPerSec: 1626.35
BytesPerReq: 1117.09
BusyWorkers: 34
IdleWorkers: 6
Scoreboard: WWWWWWWW_WW_WWWWWWWWWWWWW_WWW_WWWWWWW__W
	`)
	return metric
}

func TestDefaultConfigNerveHTTPD(t *testing.T) {
	collector := getNerveHTTPDCollector()
	collector.Configure(make(map[string]interface{}))

	assert.Equal(t, 10, collector.Interval())
	assert.Equal(t, "/etc/nerve/nerve.conf.json", collector.configFilePath)
	assert.Equal(t, "server-status?auto", collector.queryPath)
	assert.Equal(t, time.Duration(1)*time.Hour, collector.statusTTL)
}

func TestCustomConfigNerveHTTPD(t *testing.T) {
	collector := getNerveHTTPDCollector()
	configMap := map[string]interface{}{
		"status_ttl":     120,
		"configFilePath": "/tmp/foobar",
	}
	collector.Configure(configMap)

	assert.Equal(t, 10, collector.Interval())
	assert.Equal(t, "/tmp/foobar", collector.configFilePath)
	assert.Equal(t, "server-status?auto", collector.queryPath)
	assert.Equal(t, time.Duration(120)*time.Second, collector.statusTTL)
}

func TestExtractApacheMetrics(t *testing.T) {
	metrics := extractApacheMetrics(getRawApacheStat())
	metricMap := map[string]metric.Metric{}
	for _, m := range metrics {
		metricMap[m.Name] = m
	}
	assert.Equal(t, 99.0, metricMap["TotalAccesses"].Value)
	assert.Equal(t, 34.0, metricMap["WritingWorkers"].Value)
	assert.Equal(t, 6.0, metricMap["IdleWorkers"].Value)
	assert.Equal(t, 6.0, metricMap["StandbyWorkers"].Value)
	assert.Equal(t, 901.485, metricMap["CPULoad"].Value)
	assert.Equal(t, 1.45588, metricMap["ReqPerSec"].Value)
	assert.Equal(t, 1626.35, metricMap["BytesPerSec"].Value)
}

func TestFetchApacheMetrics(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	defer ts.Close()
	endpoint := ts.URL + "/server-status?auto=close"
	httpResponse := fetchApacheMetrics(endpoint, 10)
	assert.Equal(t, 404, httpResponse.status)
}

func TestNerveHTTPDCollect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rsp *http.Request) {
		fmt.Fprint(w, string(getRawApacheStat()))
	}))
	defer server.Close()
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

	inst := getNerveHTTPDCollector()
	inst.Configure(cfg)

	inst.Collect()
	actual := []metric.Metric{}
	for i := 0; i < 17; i++ {
		actual = append(actual, <-inst.Channel())
	}
	metricMap := map[string]metric.Metric{}
	for _, m := range actual {
		metricMap[m.Name] = m
	}
	assert.Equal(t, 99.0, metricMap["TotalAccesses"].Value)
	assert.Equal(t, 34.0, metricMap["WritingWorkers"].Value)
	assert.Equal(t, 6.0, metricMap["IdleWorkers"].Value)
	assert.Equal(t, 6.0, metricMap["StandbyWorkers"].Value)
	assert.Equal(t, 901.485, metricMap["CPULoad"].Value)
	assert.Equal(t, 1.45588, metricMap["ReqPerSec"].Value)
	assert.Equal(t, 1626.35, metricMap["BytesPerSec"].Value)

	assert.Equal(t, port, metricMap["TotalAccesses"].Dimensions["port"])
	assert.Equal(t, "test_service", metricMap["TotalAccesses"].Dimensions["service"])
}
