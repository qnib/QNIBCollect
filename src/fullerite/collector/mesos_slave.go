package collector

import (
	"encoding/json"
	"fmt"
	"fullerite/config"
	"fullerite/metric"
	"fullerite/util"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	l "github.com/Sirupsen/logrus"
)

const (
	httpDefaultTimeout            = 10
	mesosDefaultSlaveSnapshotPort = 5051
)

// Dependency injection: Makes writing unit tests much easier, by being able to override these values in the *_test.go files.
var (
	getSlaveExternalIP = util.ExternalIP
	getSlaveMetrics    = (*MesosSlaveStats).getSlaveMetrics

	getSlaveMetricsURL = func(m *MesosSlaveStats, ip string) string {
		return fmt.Sprintf("http://%s:%d/metrics/snapshot", ip, m.snapshotPort)
	}
)

// All mesos metrics are gauges except the ones in this list
var mesosSlaveCumulativeCountersList = map[string]int{
	"slave.executors_terminated":       0,
	"slave.tasks_failed":               0,
	"slave.tasks_finished":             0,
	"slave.tasks_killed":               0,
	"slave.tasks_lost":                 0,
	"slave.invalid_framework_messages": 0,
	"slave.invalid_status_udpates":     0,
	"slave.valid_framework_messages":   0,
	"slave.valid_status_udpates":       0,
}

// MesosSlaveStats Collector for mesos leader stats.
type MesosSlaveStats struct {
	baseCollector
	IP           string
	client       http.Client
	snapshotPort int
}

// NewMesosSlaveStats Simple constructor to set properties for the embedded baseCollector.
func NewMesosSlaveStats(channel chan metric.Metric, intialInterval int, log *l.Entry) *MesosSlaveStats {
	m := new(MesosSlaveStats)

	m.log = log
	m.channel = channel
	m.interval = intialInterval
	m.name = "MesosSlaveStats"
	m.snapshotPort = mesosDefaultSlaveSnapshotPort
	m.client = http.Client{Timeout: time.Duration(httpDefaultTimeout) * time.Second}

	if ip, err := getSlaveExternalIP(); err != nil {
		m.log.Error("Cannot determine IP: ", err.Error())
	} else {
		m.IP = ip
	}

	return m
}

// Configure Override *baseCollector.Configure(). Will create the required MesosLeaderElect instance.
func (m *MesosSlaveStats) Configure(configMap map[string]interface{}) {
	m.configureCommonParams(configMap)
	c := config.GetAsMap(configMap)

	if httpTimeout, exists := c["httpTimeout"]; exists {
		m.client.Timeout = time.Duration(config.GetAsInt(httpTimeout, httpDefaultTimeout)) * time.Second
	}

	if slaveSnapshotPort, exists := c["slaveSnapshotPort"]; exists {
		m.snapshotPort = config.GetAsInt(slaveSnapshotPort, mesosDefaultSlaveSnapshotPort)
	}
}

// Collect Compares box IP against leader IP and if true, sends data.
func (m *MesosSlaveStats) Collect() {
	if m.IP == "" {
		m.log.Error("Cannot get external IP. Skipping collection.")
		return
	}
	go m.sendMetrics()
}

// sendMetrics Send to baseCollector channel.
func (m *MesosSlaveStats) sendMetrics() {
	for metricName, value := range getSlaveMetrics(m, m.IP) {
		s := m.buildMetric(metricName, value)

		m.Channel() <- s
	}
}

// getMetrics Get metrics from the :5051/metrics/snapshot mesos endpoint.
func (m *MesosSlaveStats) getSlaveMetrics(ip string) map[string]float64 {
	url := getSlaveMetricsURL(m, ip)
	r, err := m.client.Get(url)

	if err != nil {
		m.log.Error("Could not load metrics from mesos", err.Error())
		return nil
	}

	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		m.log.Error("Not 200 response code from mesos: ", r.Status)
		return nil
	}

	contents, _ := ioutil.ReadAll(r.Body)
	raw := strings.Replace(string(contents), "\\/", ".", -1)

	var snapshot map[string]float64
	decodeErr := json.Unmarshal([]byte(raw), &snapshot)

	if decodeErr != nil {
		m.log.Error("Unable to decode mesos metrics JSON: ", decodeErr.Error())
		return nil
	}

	return snapshot
}

// buildMetric creates the metric and set the correct metricType
func (m *MesosSlaveStats) buildMetric(name string, value float64) metric.Metric {
	s := metric.New("mesos." + name)
	s.Value = value
	if _, exists := mesosSlaveCumulativeCountersList[name]; exists {
		s.MetricType = metric.CumulativeCounter
	}
	return s
}
