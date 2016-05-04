package collector

import (
	"fullerite/metric"
	"time"

	l "github.com/Sirupsen/logrus"
)

// NerveHTTPD discovers Apache servers via Nerve config
// and reports metric for them
type NerveHTTPD struct {
	baseCollector

	configFilePath string
	queryPath      string
	timeout        int
	statusTTL      time.Duration
}

func init() {
	RegisterCollector("NerveHTTPD", newNerveHTTPD)
}

func newNerveHTTPD(channel chan metric.Metric, initialInterval int, log *l.Entry) Collector {
	c := new(NerveHTTPD)
	c.channel = channel
	c.interval = initialInterval
	c.log = log

	c.name = collectorName
	c.configFilePath = "/etc/nerve/nerve.conf.json"
	c.queryPath = "server-status?auto"
	c.timeout = 2
	c.statusTTL = time.Duration(60) * time.Minute

	return c
}

// Configure the collector
func (c *NerveHTTPD) Configure(configMap map[string]interface{}) {
	if val, exists := configMap["queryPath"]; exists {
		c.queryPath = val.(string)
	}
	if val, exists := configMap["configFilePath"]; exists {
		c.configFilePath = val.(string)
	}

	if val, exists := configMap["status_ttl"]; exists {
		if t, ok := val.(int); ok {
			c.statusTTL = time.Duration(t) * time.Second
		}
	}

	c.configureCommonParams(configMap)
}

// Collect the metrics
func (c *NerveHTTPD) Collect() {

}
