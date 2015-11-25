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

// DI
var (
	externalIP = util.ExternalIP

	sendMetrics = (*MesosStats).sendMetrics
	getMetrics  = (*MesosStats).getMetrics

	newMLE        = func() MesosLeaderElectInterface { return new(MesosLeaderElect) }
	getMetricsURL = func(ip string) string { return fmt.Sprintf("http://%s:5050/metrics/snapshot", ip) }
)

const (
	cacheTimeout = 5 * time.Minute
	getTimeout   = 5 * time.Second
)

// MesosStats Collector for mesos leader stats.
type MesosStats struct {
	baseCollector
	IP         string
	client     http.Client
	mesosCache MesosLeaderElectInterface
}

// NewMesosStats Simple constructor to set properties for the embedded baseCollector.
func NewMesosStats(channel chan metric.Metric, intialInterval int, log *l.Entry) *MesosStats {
	m := new(MesosStats)

	m.log = log
	m.channel = channel
	m.interval = intialInterval
	m.name = "MesosStats"
	m.client = http.Client{Timeout: getTimeout}

	if ip, err := externalIP(); err != nil {
		m.log.Error("Cannot determine internal IP")
	} else {
		m.IP = ip
	}

	return m
}

// Configure Override *baseCollector.Configure(). Will create the required MesosLeaderElect instance.
func (m *MesosStats) Configure(configMap map[string]interface{}) {
	m.configureCommonParams(configMap)

	c := config.GetAsMap(configMap)
	mesosNodes, exists := c["mesosNodes"]

	if !exists || len(mesosNodes) == 0 {
		m.log.Error("Require configuration not found: mesosNodes")
		return
	}

	m.mesosCache = newMLE()
	m.mesosCache.Configure(mesosNodes, cacheTimeout)
}

// Collect Compares box IP against leader IP and if true, sends data.
func (m *MesosStats) Collect() {
	switch m.mesosCache {
	case nil:
		m.log.Error("No mesosCache, Configure() probably failed.")
		return
	default:
		if m.mesosCache.Get() != m.IP {
			m.log.Warn("Not the leader; skipping.")
			return
		}
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
	json.Unmarshal([]byte(raw), &snapshot)

	return snapshot
}

// buildMetric Build a fullerite metric.
func buildMetric(k string, v float64) metric.Metric {
	m := metric.New(k)
	m.Value = v

	return m
}
