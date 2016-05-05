package collector

import (
	"fullerite/metric"
	"testing"

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
}

func TestExtractApacheMetrics(t *testing.T) {
	metrics := extractApacheMetrics(getRawApacheStat())
	metricMap := map[string]metric.Metric{}
	for _, m := range metrics {
		metricMap[m.Name] = m
	}
	assert.Equal(t, 68, metricMap["Uptime"])
}
