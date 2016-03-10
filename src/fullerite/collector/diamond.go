package collector

import (
	"fullerite/metric"

	"bufio"
	"encoding/json"
	"net"
	"strings"
	"time"

	l "github.com/Sirupsen/logrus"
)

const (
	// DefaultDiamondCollectorPort is the TCP port that diamond
	// collectors write to and we read off of.
	DefaultDiamondCollectorPort = "19191"
)

// Diamond collector type
type Diamond struct {
	baseCollector
	port           string
	serverStarted  bool
	metricCounters map[string]float64
	incoming       chan []byte
}

func init() {
	RegisterCollector("Diamond", newDiamond)
}

// newDiamond creates a new Diamond collector.
func newDiamond(channel chan metric.Metric, initialInterval int, log *l.Entry) Collector {
	d := new(Diamond)

	d.log = log
	d.channel = channel
	d.interval = initialInterval

	d.name = "Diamond"
	d.incoming = make(chan []byte)
	d.port = DefaultDiamondCollectorPort
	d.serverStarted = false
	d.SetCollectorType("listener")
	d.metricCounters = make(map[string]float64)
	return d
}

// Configure the collector
func (d *Diamond) Configure(configMap map[string]interface{}) {
	if port, exists := configMap["port"]; exists {
		d.port = port.(string)
	}
	d.configureCommonParams(configMap)
}

// Port returns Diamond collectors listen port
func (d *Diamond) Port() string {
	return d.port
}

// collectDiamond opens up and reads from the a TCP socket and
// writes what it's read to a local channel. Diamond handler (running in
// separate processes) write to the same port.
//
// When Collect() is called it reads from the local channel converts
// strings to metrics and publishes metrics to handlers.
func (d *Diamond) collectDiamond() {
	addr, err := net.ResolveTCPAddr("tcp", ":"+d.port)

	if err != nil {
		panic(err)
	}

	l, err := net.ListenTCP("tcp4", addr)
	if err != nil {
		d.log.Fatal("Cannot listen on diamond socket", err)
	}

	// figure out the port bind for Port()
	d.port = strings.Split(l.Addr().String(), ":")[1]

	for {
		conn, err := l.AcceptTCP()
		if err != nil {
			d.log.Fatal(err)
		}
		go d.readDiamondMetrics(conn)
	}
}

// readDiamondMetrics reads from the connection
func (d *Diamond) readDiamondMetrics(conn *net.TCPConn) {
	defer conn.Close()
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Second)
	reader := bufio.NewReader(conn)
	d.log.Info("Connection started: ", conn.RemoteAddr())
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			d.log.Warn("Error while reading diamond metrics", err)
			break
		}
		d.log.Debug("Read: ", string(line))
		d.incoming <- line
	}
	d.log.Info("Connection closed: ", conn.RemoteAddr())
}

// Collect reads metrics collected from Diamond collectors, converts
// them to fullerite's Metric type and publishes them to handlers.
func (d *Diamond) Collect() {
	if !d.serverStarted {
		d.serverStarted = true
		go d.collectDiamond()
	}

	for line := range d.incoming {
		if metrics, ok := d.parseMetrics(line); ok {
			for _, metric := range metrics {
				if metric.Name == "metric_emission" {
					d.addToMetricCounter(metric)
				} else {
					d.Channel() <- metric
				}

			}
		}
	}
}

func (d *Diamond) addToMetricCounter(metric metric.Metric) {
	if val, ok := metric.GetDimensionValue("collectorCanonicalName"); ok {
		d.metricCounters[val] = metric.Value
	}
}

func (d *Diamond) parseMetrics(line []byte) ([]metric.Metric, bool) {
	var metrics []metric.Metric
	if err := json.Unmarshal(line, &metrics); err != nil {
		d.log.Error("Cannot unmarshal metric line from diamond:", line)
		return metrics, false
	}
	// All diamond metric_types are reported in uppercase, lets make them
	// fullerite compatible
	for i := range metrics {
		metrics[i].MetricType = strings.ToLower(metrics[i].MetricType)
		metrics[i].AddDimension("diamond", "yes")
	}
	return metrics, true
}

// InternalMetrics returns internal metrics of collector
func (d *Diamond) InternalMetrics() map[string]metric.InternalMetrics {
	metricStats := map[string]metric.InternalMetrics{}
	for k, v := range d.metricCounters {
		counters := map[string]float64{}

		gauges := map[string]float64{
			"metric_emission": float64(v),
		}

		m := metric.InternalMetrics{
			Counters: counters,
			Gauges:   gauges,
		}
		metricStats[k] = m
	}
	return metricStats
}
