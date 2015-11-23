package collector

import (
	"fullerite/config"
	"fullerite/metric"
	"time"

	l "github.com/Sirupsen/logrus"
)

const (
	cacheTimeout = 5 * time.Minute
)

// MesosStats Collector for mesos leader stats.
type MesosStats struct {
	baseCollector
	mesosCache MesosLeaderElectInterface
}

// NewMesosStats Simple constructor to set properties for the embedded baseCollector.
func NewMesosStats(channel chan metric.Metric, intialInterval int, log *l.Entry) *MesosStats {
	m := new(MesosStats)

	m.log = log
	m.channel = channel
	m.interval = intialInterval
	m.name = "MesosStats"

	return m
}

// Configure Override *baseCollector.Configure(). Will create the required MesosLeaderElect instance.
func (m *MesosStats) Configure(configMap map[string]interface{}) {
	c := config.GetAsMap(configMap)

	if mesosNodes, exists := c["mesosNodes"]; !exists || len(mesosNodes) == 0 {
		m.log.Error("Require configuration not found: mesosNodes")
		return
	} else {
		m.mesosCache = new(MesosLeaderElect)
		m.mesosCache.Configure(mesosNodes, cacheTimeout)
	}
}
