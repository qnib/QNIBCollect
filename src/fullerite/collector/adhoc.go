package collector

import (
	"fmt"
	"os/user"

	"fullerite/metric"

	l "github.com/Sirupsen/logrus"
)

// AdHoc collector type
type AdHoc struct {
	baseCollector
	metricPrefix  string
	collectorFile string
}

// NewAdHoc Simple constructor for an AdHoc collector
func NewAdHoc(channel chan metric.Metric, initialInterval int, log *l.Entry) *AdHoc {
	a := new(AdHoc)
	a.channel = channel
	a.interval = initialInterval
	a.log = log

	a.name = "AdHoc"
	currentUser, _ := user.Current()
	a.metricPrefix = "adhoc." + currentUser.Username + "."
	return a
}

// Configure Override default parameters
func (a *AdHoc) Configure(configMap map[string]interface{}) {
	if collectorFile, exists := configMap["collectorFile"]; exists {
		a.collectorFile = collectorFile.(string)
	}
	a.configureCommonParams(configMap)

	fmt.Println(a, a.name, a.metricPrefix, a.interval, a.collectorFile)
}

// Collect Emits the metrics produce by the AdHoc script
func (a AdHoc) Collect() {
	a.log.Info("Collecting...")
	//metric := metric.New(c.metricName)
	//metric.Value = value
	//metric.AddDimension("model", model)
	//c.Channel() <- metric
	//c.log.Debug(metric)
}
