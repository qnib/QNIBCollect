package handler

import (
	"fullerite/config"
	"fullerite/metric"

	"encoding/json"
	"fmt"
	"net"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/samuel/go-thrift/examples/scribe"
	"github.com/samuel/go-thrift/thrift"
)

type fulleriteScribeClient interface {
	Log(Messages []*scribe.LogEntry) (scribe.ResultCode, error)
}

// Scribe Handler
type Scribe struct {
	BaseHandler
	endpoint     string
	port         int
	streamName   string
	scribeClient fulleriteScribeClient
}

type scribeMetric struct {
	Name       string            `json:"name"`
	MetricType string            `json:"type"`
	Value      float64           `json:"value"`
	Timestamp  int64             `json:"timestamp"`
	Dimensions map[string]string `json:"dimensions"`
}

const (
	defaultScribeEndpoint   = "localhost"
	defaultScribePort       = 1464
	defaultScribeStreamName = "fullerite_to_scribe"
)

// NewScribe returns a new Scribe handler.
func NewScribe(
	channel chan metric.Metric,
	initialInterval int,
	initialBufferSize int,
	initialTimeout time.Duration,
	log *l.Entry) *Scribe {

	inst := new(Scribe)
	inst.name = "Scribe"

	inst.interval = initialInterval
	inst.maxBufferSize = initialBufferSize
	inst.timeout = initialTimeout
	inst.log = log
	inst.channel = channel

	inst.endpoint = defaultScribeEndpoint
	inst.port = defaultScribePort
	inst.streamName = defaultScribeStreamName

	return inst
}

// Configure accepts the different configuration options for the Scribe handler
func (s *Scribe) Configure(configMap map[string]interface{}) {
	if endpoint, exists := configMap["endpoint"]; exists {
		s.endpoint = endpoint.(string)
	}

	if port, exists := configMap["port"]; exists {
		s.port = config.GetAsInt(port, defaultScribePort)
	}

	if stream, exists := configMap["streamName"]; exists {
		s.streamName = stream.(string)
	}

	s.configureCommonParams(configMap)
}

// Run runs the handler main loop
func (s *Scribe) Run() {
	server := fmt.Sprintf("%s:%d", s.endpoint, s.port)
	conn, err := net.Dial("tcp", server)

	if err != nil {
		s.log.Errorf("Failed to connect to %s. Error: %s", server, err.Error())

	} else {
		t := thrift.NewTransport(thrift.NewFramedReadWriteCloser(conn, 0), thrift.BinaryProtocol)
		client := thrift.NewClient(t, false)
		s.scribeClient = &scribe.ScribeClient{Client: client}
	}

	s.run(s.emitMetrics)
}

func (s *Scribe) emitMetrics(metrics []metric.Metric) bool {
	s.log.Info("Starting to emit ", len(metrics), " metrics")

	if s.scribeClient == nil {
		s.log.Warn("Cannot connect to scribe server. Skipping send.")
		return false
	}

	if len(metrics) == 0 {
		s.log.Warn("Skipping send because of an empty payload")
		return false
	}

	var encodedMetrics []*scribe.LogEntry
	for _, m := range metrics {
		jsonMetric, err := json.Marshal(createScribeMetric(m))
		if err != nil {
			s.log.Warnf("JSON encode failed: %s", err.Error())
		} else {
			encodedMetrics = append(encodedMetrics, &scribe.LogEntry{s.streamName, string(jsonMetric)})
		}
	}

	if len(encodedMetrics) > 0 {
		_, err := s.scribeClient.Log(encodedMetrics)

		if err != nil {
			s.log.Errorf("Failed to write to scribe. Error: %s", err.Error())
			return false
		}
	}

	s.log.Info("Successfully written ", len(encodedMetrics), " datapoints to Scribe")
	return true
}

func createScribeMetric(m metric.Metric) scribeMetric {
	return scribeMetric{
		Name:       m.Name,
		Value:      m.Value,
		MetricType: m.MetricType,
		Timestamp:  time.Now().Unix(),
		Dimensions: m.Dimensions,
	}
}
