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

// Dependency injection: Makes writing unit tests much easier, by being able to override these values in the *_test.go files.
var (
	externalIP = util.ExternalIP

	sendMetrics = (*MesosStats).sendMetrics
	getMetrics  = (*MesosStats).getMetrics

	newMLE        = func() util.MesosLeaderElectInterface { return new(util.MesosLeaderElect) }
	getMetricsURL = func(ip string) string { return fmt.Sprintf("http://%s:5050/metrics/snapshot", ip) }
)

// All mesos metrics are gauges except the ones in this list
var mesosMasterCumulativeCountersList = map[string]int{
	"master.slave_registrations":                    0,
	"master.slave_removals":                         0,
	"master.slave_reregistrations":                  0,
	"master.slave_shutdowns_scheduled":              0,
	"master.slave_shutdowns_cancelled":              0,
	"master.slave_shutdowns_completed":              0,
	"master.tasks_error":                            0,
	"master.tasks_failed":                           0,
	"master.tasks_finished":                         0,
	"master.tasks_killed":                           0,
	"master.tasks_lost":                             0,
	"master.invalid_framework_to_executor_messages": 0,
	"master.invalid_status_update_acknowledgements": 0,
	"master.invalid_status_updates":                 0,
	"master.dropped_messages":                       0,
	"master.messages_authenticate":                  0,
	"master.messages_deactivate_framework":          0,
	"master.messages_exited_executor":               0,
	"master.messages_framework_to_executor":         0,
	"master.messages_kill_task":                     0,
	"master.messages_launch_tasks":                  0,
	"master.messages_reconcile_tasks":               0,
	"master.messages_register_framework":            0,
	"master.messages_register_slave":                0,
	"master.messages_reregister_framework":          0,
	"master.messages_reregister_slave":              0,
	"master.messages_resource_request":              0,
	"master.messages_revive_offers":                 0,
	"master.messages_status_udpate":                 0,
	"master.messages_status_update_acknowledgement": 0,
	"master.messages_unregister_framework":          0,
	"master.messages_unregister_slave":              0,
	"master.valid_framework_to_executor_messages":   0,
	"master.valid_status_update_acknowledgements":   0,
	"master.valid_status_updates":                   0,
}

const (
	cacheTimeout = 5 * time.Minute
	getTimeout   = 10 * time.Second
)

// MesosStats Collector for mesos leader stats.
type MesosStats struct {
	baseCollector
	IP         string
	client     http.Client
	mesosCache util.MesosLeaderElectInterface
}

func init() {
	RegisterCollector("MesosStats", newMesosStats)
}

// newMesosStats Simple constructor to set properties for the embedded baseCollector.
func newMesosStats(channel chan metric.Metric, intialInterval int, log *l.Entry) Collector {
	m := new(MesosStats)

	m.log = log
	m.channel = channel
	m.interval = intialInterval
	m.name = "MesosStats"
	m.client = http.Client{Timeout: getTimeout}

	if ip, err := externalIP(); err != nil {
		m.log.Error("Cannot determine IP: ", err.Error())
	} else {
		m.IP = ip
	}

	return m
}

// Configure Override *baseCollector.Configure(). Will create the required MesosLeaderElect instance.
func (m *MesosStats) Configure(configMap map[string]interface{}) {
	m.configureCommonParams(configMap)

	c := config.GetAsMap(configMap)
	if mesosNodes, exists := c["mesosNodes"]; exists && len(mesosNodes) > 0 {
		m.mesosCache = newMLE()
		m.mesosCache.Configure(mesosNodes, cacheTimeout)
	} else {
		m.log.Error("Require configuration not found: mesosNodes")
		return
	}
}

// Collect Compares box IP against leader IP and if true, sends data.
func (m *MesosStats) Collect() {
	if m.mesosCache == nil {
		m.log.Error("No mesosCache, Configure() probably failed.")
		return
	} else if m.mesosCache.Get() != m.IP {
		m.log.Warn("Not the leader; skipping.")
		return
	}

	go sendMetrics(m)
}

// sendMetrics Send to baseCollector channel.
func (m *MesosStats) sendMetrics() {
	for k, v := range getMetrics(m, m.IP) {
		s := buildMetric(k, v)
		m.Channel() <- s
	}
}

// getMetrics Get metrics from the :5050/metrics/snapshot mesos endpoint.
func (m *MesosStats) getMetrics(ip string) map[string]float64 {
	url := getMetricsURL(ip)
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

// buildMetric Build a fullerite metric.
func buildMetric(k string, v float64) metric.Metric {
	m := metric.New("mesos." + k)
	m.Value = v

	if _, exists := mesosMasterCumulativeCountersList[k]; exists {
		m.MetricType = metric.CumulativeCounter
	}

	return m
}
