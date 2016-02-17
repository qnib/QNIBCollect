package handler

import (
	"fullerite/metric"

	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	l "github.com/Sirupsen/logrus"
)

func init() {
	RegisterHandler("Datadog", newDatadog)
}

// Datadog handler
type Datadog struct {
	BaseHandler
	endpoint string
	apiKey   string
}

type datadogPayload struct {
	Series []datadogMetric `json:"series"`
}

type datadogMetric struct {
	Metric     string         `json:"metric"`
	Points     []datadogPoint `json:"points"`
	MetricType string         `json:"type"`
	Host       string         `json:"host"`
	Tags       []string       `json:"tags"`
}

type datadogPoint [2]float64

// newDatadog returns a new Datadog handler
func newDatadog(
	channel chan metric.Metric,
	initialInterval int,
	initialBufferSize int,
	initialTimeout time.Duration,
	log *l.Entry) Handler {

	inst := new(Datadog)
	inst.name = "Datadog"

	inst.interval = initialInterval
	inst.maxBufferSize = initialBufferSize
	inst.timeout = initialTimeout
	inst.log = log
	inst.channel = channel
	return inst
}

// Configure the Datadog handler
func (d *Datadog) Configure(configMap map[string]interface{}) {
	if apiKey, exists := configMap["apiKey"]; exists {
		d.apiKey = apiKey.(string)
	} else {
		d.log.Error("There was no API key specified for the Datadog handler, there won't be any emissions")
	}
	if endpoint, exists := configMap["endpoint"]; exists {
		d.endpoint = endpoint.(string)
	} else {
		d.log.Error("There was no endpoint specified for the Datadog Handler, there won't be any emissions")
	}
	d.configureCommonParams(configMap)
}

// Endpoint returns the Datadog API endpoint
func (d Datadog) Endpoint() string {
	return d.endpoint
}

// Run runs the handler main loop
func (d *Datadog) Run() {
	d.run(d.emitMetrics)
}

func (d *Datadog) convertToDatadog(incomingMetric metric.Metric) (datapoint datadogMetric) {
	dog := new(datadogMetric)
	dog.Metric = d.Prefix() + incomingMetric.Name
	dog.Points = makeDatadogPoints(incomingMetric)
	dog.MetricType = incomingMetric.MetricType

	// first check the defaults
	if host, ok := d.DefaultDimensions()["host"]; ok {
		dog.Host = host
	} else if host, ok := incomingMetric.GetDimensionValue("host"); ok {
		dog.Host = host
	} else {
		dog.Host = "unknown"
	}

	dog.Tags = d.serializedDimensions(incomingMetric)
	return *dog
}

func (d *Datadog) emitMetrics(metrics []metric.Metric) bool {
	d.log.Info("Starting to emit ", len(metrics), " metrics")

	if len(metrics) == 0 {
		d.log.Warn("Skipping send because of an empty payload")
		return false
	}

	series := make([]datadogMetric, 0, len(metrics))
	for _, m := range metrics {
		series = append(series, d.convertToDatadog(m))
	}

	p := datadogPayload{Series: series}
	payload, err := json.Marshal(p)
	if err != nil {
		d.log.Error("Failed marshaling datapoints to Datadog format")
		d.log.Error("Dropping Datadog datapoints ", series)
		return false
	}

	apiURL := fmt.Sprintf("%s/series?api_key=%s", d.endpoint, d.apiKey)
	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payload))
	if err != nil {
		d.log.Error("Failed to create a request to endpoint ", d.endpoint)
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	transport := http.Transport{
		Dial: d.dialTimeout,
	}
	client := &http.Client{
		Transport: &transport,
	}
	rsp, err := client.Do(req)
	if err != nil {
		d.log.Error("Failed to complete POST ", err)
		return false
	}

	defer rsp.Body.Close()
	if (rsp.StatusCode == http.StatusOK) || (rsp.StatusCode == http.StatusAccepted) {
		d.log.Info("Successfully sent ", len(series), " datapoints to Datadog")
		return true
	}

	body, _ := ioutil.ReadAll(rsp.Body)
	d.log.Error("Failed to post to Datadog @", d.endpoint,
		" status was ", rsp.Status,
		" rsp body was ", string(body),
		" payload was ", string(payload))
	return false
}

func (d Datadog) dialTimeout(network, addr string) (net.Conn, error) {
	return net.DialTimeout(network, addr, d.timeout)
}

func (d Datadog) serializedDimensions(m metric.Metric) (dimensions []string) {
	for name, value := range m.GetDimensions(d.DefaultDimensions()) {
		dimensions = append(dimensions, name+":"+value)
	}
	return dimensions
}

func makeDatadogPoints(m metric.Metric) []datadogPoint {
	point := datadogPoint{float64(time.Now().Unix()), m.Value}
	return []datadogPoint{point}
}
