package collector

import (
	"fullerite/metric"

	l "github.com/Sirupsen/logrus"
)

// NerveHTTPD discovers Apache servers via Nerve config
// and reports metric for them
type NerveHTTPD struct {
	baseCollector
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
	return c
}

// Configure the collector
func (c *NerveHTTPD) Configure(configMap map[string]interface{}) {

}

// Collect the metrics
func (c *NerveHTTPD) Collect() {

}
