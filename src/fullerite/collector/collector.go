package collector

import (
	"fullerite/config"
	"fullerite/metric"

	l "github.com/Sirupsen/logrus"
)

const (
	// DefaultCollectionInterval the interval to collect on unless overridden by a collectors config
	DefaultCollectionInterval = 10
	// CPUInfoCollectionInterval default collection interval for the CPUInfo collector
	CPUInfoCollectionInterval = 3600
)

var defaultLog = l.WithFields(l.Fields{"app": "fullerite", "pkg": "collector"})

// Collector defines the interface of a generic collector.
type Collector interface {
	Collect()
	Configure(map[string]interface{})

	// taken care of by the base class
	Name() string
	Channel() chan metric.Metric
	Interval() int
	SetInterval(int)
	CollectorType() string
	SetCollectorType(string)
}

// New creates a new Collector based on the requested collector name.
func New(name string) Collector {
	var collector Collector

	channel := make(chan metric.Metric)
	collectorLog := defaultLog.WithFields(l.Fields{"collector": name})

	switch name {
	case "Test":
		collector = NewTest(channel, DefaultCollectionInterval, collectorLog)
	case "Diamond":
		collector = NewDiamond(channel, DefaultCollectionInterval, collectorLog)
		collector.SetCollectorType("listener")
	case "Fullerite":
		collector = NewFullerite(channel, DefaultCollectionInterval, collectorLog)
	case "ProcStatus":
		collector = NewProcStatus(channel, DefaultCollectionInterval, collectorLog)
	case "FulleriteHTTP":
		collector = newFulleriteHTTPCollector(channel, DefaultCollectionInterval, collectorLog)
		collector.SetCollectorType("listener")
	case "NerveUWSGI":
		collector = newNerveUWSGICollector(channel, DefaultCollectionInterval, collectorLog)
	case "DockerStats":
		collector = NewDockerStats(channel, DefaultCollectionInterval, collectorLog)
	case "CpuInfo":
		collector = NewCPUInfo(channel, CPUInfoCollectionInterval, collectorLog)
	case "MesosStats":
		collector = NewMesosStats(channel, DefaultCollectionInterval, collectorLog)
	case "MesosSlaveStats":
		collector = NewMesosSlaveStats(channel, DefaultCollectionInterval, collectorLog)
	case "MySQLBinlogGrowth":
		collector = NewMySQLBinlogGrowth(channel, DefaultCollectionInterval, collectorLog)
	case "AdHoc":
		collector = NewAdHoc(channel, DefaultCollectionInterval, collectorLog)
		collector.SetCollectorType("listener")
	default:
		defaultLog.Error("Cannot create collector: ", name)
		return nil
	}
	if collector.CollectorType() == "" {
		collector.SetCollectorType("collector")
	}
	return collector
}

type baseCollector struct {
	// fulfill most of the rote parts of the collector interface
	channel       chan metric.Metric
	name          string
	interval      int
	collectorType string

	// intentionally exported
	log *l.Entry
}

func (col *baseCollector) configureCommonParams(configMap map[string]interface{}) {
	if interval, exists := configMap["interval"]; exists {
		col.interval = config.GetAsInt(interval, DefaultCollectionInterval)
	}
}

// SetInterval : set the interval to collect on
func (col *baseCollector) SetInterval(interval int) {
	col.interval = interval
}

// SetCollectorType : collector type
func (col *baseCollector) SetCollectorType(collectorType string) {
	col.collectorType = collectorType
}

// CollectorType : collector type
func (col *baseCollector) CollectorType() string {
	return col.collectorType
}

// Channel : the channel on which the collector should send metrics
func (col baseCollector) Channel() chan metric.Metric {
	return col.channel
}

// Name : the name of the collector
func (col baseCollector) Name() string {
	return col.name
}

// Interval : the interval to collect the metrics on
func (col baseCollector) Interval() int {
	return col.interval
}

// String returns the collector name in printable format.
func (col baseCollector) String() string {
	return col.Name() + "Collector"
}
