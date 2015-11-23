package handler

import (
	"fullerite/metric"

	"bytes"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
)

// SignalFx Handler
type SignalFx struct {
	BaseHandler
	endpoint  string
	authToken string
}

// NewSignalFx returns a new SignalFx handler.
func NewSignalFx(
	channel chan metric.Metric,
	initialInterval int,
	initialBufferSize int,
	initialTimeout time.Duration,
	log *l.Entry) *SignalFx {

	inst := new(SignalFx)
	inst.name = "SignalFx"

	inst.interval = initialInterval
	inst.maxBufferSize = initialBufferSize
	inst.timeout = initialTimeout
	inst.log = log
	inst.channel = channel

	return inst
}

// Configure accepts the different configuration options for the signalfx handler
func (s *SignalFx) Configure(configMap map[string]interface{}) {
	if authToken, exists := configMap["authToken"]; exists {
		s.authToken = authToken.(string)
	} else {
		s.log.Error("There was no auth key specified for the SignalFx Handler, there won't be any emissions")
	}
	if endpoint, exists := configMap["endpoint"]; exists {
		s.endpoint = endpoint.(string)
	} else {
		s.log.Error("There was no endpoint specified for the SignalFx Handler, there won't be any emissions")
	}

	s.configureCommonParams(configMap)
}

// Endpoint returns SignalFx' API endpoint
func (s *SignalFx) Endpoint() string {
	return s.endpoint
}

// Run runs the handler main loop
func (s *SignalFx) Run() {
	s.run(s.emitMetrics)
}

func (s *SignalFx) convertToProto(incomingMetric metric.Metric) *DataPoint {
	// Create a new values for the Datapoint that requires pointers.
	outname := s.Prefix() + incomingMetric.Name
	value := incomingMetric.Value

	datapoint := new(DataPoint)
	datapoint.Metric = &outname
	datapoint.Value = &Datum{
		DoubleValue: &value,
	}
	datapoint.Source = new(string)
	*datapoint.Source = "fullerite"

	switch incomingMetric.MetricType {
	case metric.Gauge:
		datapoint.MetricType = MetricType_GAUGE.Enum()
	case metric.Counter:
		datapoint.MetricType = MetricType_COUNTER.Enum()
	case metric.CumulativeCounter:
		datapoint.MetricType = MetricType_CUMULATIVE_COUNTER.Enum()
	}

	dimensions := incomingMetric.GetDimensions(s.DefaultDimensions())
	for key, value := range dimensions {
		// Dimension (protobuf) require a pointer to string
		// values. We need to create new string objects in the
		// scope of this for loop not to repeatedly add the
		// same key:value pairs to the the datapoint.
		dimensionKey := key
		dimensionValue := value
		dim := Dimension{
			Key:   &dimensionKey,
			Value: &dimensionValue,
		}
		datapoint.Dimensions = append(datapoint.Dimensions, &dim)
	}

	return datapoint
}

func (s *SignalFx) emitMetrics(metrics []metric.Metric) bool {
	s.log.Info("Starting to emit ", len(metrics), " metrics")

	if len(metrics) == 0 {
		s.log.Warn("Skipping send because of an empty payload")
		return false
	}

	datapoints := make([]*DataPoint, 0, len(metrics))
	for _, m := range metrics {
		datapoints = append(datapoints, s.convertToProto(m))
	}

	payload := new(DataPointUploadMessage)
	payload.Datapoints = datapoints

	if s.authToken == "" || s.endpoint == "" {
		s.log.Warn("Skipping emission because we're missing the auth token ",
			"or the endpoint, payload would have been ", payload)
		return false
	}
	serialized, err := proto.Marshal(payload)
	if err != nil {
		s.log.Error("Failed to serailize payload ", payload)
		return false
	}

	req, err := http.NewRequest("POST", s.endpoint, bytes.NewBuffer(serialized))
	if err != nil {
		s.log.Error("Failed to create a request to endpoint ", s.endpoint)
		return false
	}
	req.Header.Set("X-SF-TOKEN", s.authToken)
	req.Header.Set("Content-Type", "application/x-protobuf")

	transport := http.Transport{
		Dial: s.dialTimeout,
	}
	client := &http.Client{
		Transport: &transport,
	}
	rsp, err := client.Do(req)
	if err != nil {
		s.log.Error("Failed to complete POST ", err)
		return false
	}

	defer rsp.Body.Close()
	if rsp.Status != "200 OK" {
		body, _ := ioutil.ReadAll(rsp.Body)
		s.log.Error("Failed to post to signalfx @", s.endpoint,
			" status was ", rsp.Status,
			" rsp body was ", string(body),
			" payload was ", payload)
		return false
	}

	s.log.Info("Successfully sent ", len(datapoints), " datapoints to SignalFx")
	return true
}

func (s *SignalFx) dialTimeout(network, addr string) (net.Conn, error) {
	return net.DialTimeout(network, addr, s.timeout)
}
