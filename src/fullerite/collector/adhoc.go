package collector

import (
	"bytes"
	"os"
	"os/exec"
	"os/user"

	"encoding/json"
	"fullerite/metric"

	l "github.com/Sirupsen/logrus"
)

// AdHoc collector type
type AdHoc struct {
	baseCollector
	metricPrefix  string
	collectorFile string
}

func init() {
	RegisterCollector("AdHoc", newAdHoc)
}

// newAdHoc Simple constructor for an AdHoc collector
func newAdHoc(channel chan metric.Metric, initialInterval int, log *l.Entry) Collector {
	a := new(AdHoc)
	a.channel = channel
	a.interval = initialInterval
	a.log = log

	a.name = "AdHoc"
	currentUser, _ := user.Current()
	a.metricPrefix = "adhoc." + currentUser.Username + "."
	a.SetCollectorType("listener")
	return a
}

// Configure Override default parameters
func (a *AdHoc) Configure(configMap map[string]interface{}) {
	if collectorFile, exists := configMap["collectorFile"]; exists {
		a.collectorFile = collectorFile.(string)
		// chmod ugoa+rwx
		os.Chmod(a.collectorFile, 0777)
	}
	a.configureCommonParams(configMap)
}

// Collect Emits the metrics produce by the AdHoc script
func (a AdHoc) Collect() {
	a.log.Info("Collecting...")
	cmd := exec.Command(a.collectorFile, []string{""}...)
	output, err := cmd.Output()
	if err != nil {
		a.log.Error("Could not run command: ", err)
	}
	for _, line := range bytes.Split(bytes.Trim(output, "\n"), []byte{'\n'}) {
		if metrics, ok := a.parseMetrics(line); ok {
			for _, metric := range metrics {
				a.Channel() <- metric
			}
		}
	}
}

// Parse metrics from stdout
func (a *AdHoc) parseMetrics(line []byte) ([]metric.Metric, bool) {
	var metrics []metric.Metric
	var metric metric.Metric
	if err := json.Unmarshal(line, &metrics); err != nil {
		if err = json.Unmarshal(line, &metric); err != nil {
			a.log.Error("Cannot unmarshal metric line from adhoc collector:", line)
			return metrics, false
		}
		metrics = append(metrics, metric)
	}

	for i := range metrics {
		metrics[i].Name = a.metricPrefix + metrics[i].Name
		metrics[i].AddDimension("adhoc", "yes")
	}
	return metrics, true
}
