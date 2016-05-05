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

func TestDefaultConfigNerveHTTPD(t *testing.T) {
	collector := getNerveHTTPDCollector()
	collector.Configure(make(map[string]interface{}))

	assert.Equal(t, 10, collector.Interval())
	assert.Equal(t, "/etc/nerve/nerve.conf.json", collector.configFilePath)
	assert.Equal(t, "server-status?auto", collector.queryPath)
}
