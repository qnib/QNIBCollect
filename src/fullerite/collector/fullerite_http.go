package collector

import (
	"fullerite/internalserver"
	"fullerite/metric"

	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	l "github.com/Sirupsen/logrus"
)

const (
	defaultFulleriteHost     = "localhost"
	defaultFulleritePort     = 9090
	defaultFulleritePath     = "metrics"
	defaultFulleriteProtocol = "http"
)

// collects stats from fullerite's http endpoint
type fulleriteHTTP struct {
	baseHTTPCollector
}

// NewFulleriteHTTPCollector returns a collector meant to query fullerite's HTTP interface
func newFulleriteHTTPCollector(channel chan metric.Metric, initialInterval int, log *l.Entry) *fulleriteHTTP {
	inst := new(fulleriteHTTP)

	inst.log = log
	inst.channel = channel
	inst.interval = initialInterval

	inst.name = "FulleriteHTTP"

	inst.endpoint = fmt.Sprintf("%s://%s:%d/%s",
		defaultFulleriteProtocol,
		defaultFulleriteHost,
		defaultFulleritePort,
		defaultFulleritePath)

	inst.rspHandler = inst.handleResponse
	inst.errHandler = inst.handleError

	return inst
}

func (inst *fulleriteHTTP) Configure(configMap map[string]interface{}) {
	if endpoint, exists := configMap["endpoint"]; exists {
		inst.endpoint = endpoint.(string)
	}

	inst.configureCommonParams(configMap)
}

func (inst fulleriteHTTP) handleError(err error) {
	inst.log.Error("Failed to make GET to ", inst.endpoint, " error is: ", err)
}

// handleResponse assumes the format of the response is a JSON dictionary. It then converts
// them to individual metrics.
func (inst fulleriteHTTP) handleResponse(rsp *http.Response) []metric.Metric {
	results := []metric.Metric{}

	txt, err := ioutil.ReadAll(rsp.Body)
	defer rsp.Body.Close()
	if err == nil {
		parsedRsp, parseError := inst.parseResponseText(&txt)
		if parseError != nil {
			inst.log.Error("Failed to parse the request body '", string(txt), "' because of error: ", err)
		} else {
			results = parsedRsp
		}
	} else {
		inst.log.Error("Failed to get the body of the response because of error: ", err)
	}

	return results
}

func (inst fulleriteHTTP) buildMetrics(counters *map[string]float64, isCounter bool) []metric.Metric {
	results := make([]metric.Metric, 0, len(*counters))
	for key, val := range *counters {
		m := metric.New(key)
		m.Value = val
		if isCounter {
			m.MetricType = metric.CumulativeCounter
		}
		results = append(results, m)
	}
	return results
}

// parseResponseText takes the raw JSON string and parses that into metrics. The
// format of the JSON string is assumed to be a dictionary and then each key
// creates a metric.
func (inst fulleriteHTTP) parseResponseText(raw *[]byte) ([]metric.Metric, error) {
	var parsedRsp internalserver.ResponseFormat

	err := json.Unmarshal(*raw, &parsedRsp)
	if err != nil {
		return []metric.Metric{}, err
	}

	appendHandlerDim := func(metrics *[]metric.Metric, handlerName string) {
		for _, m := range *metrics {
			m.AddDimension("handler", handlerName)
		}
	}

	results := []metric.Metric{}
	// first all the memory parts create metrics
	memCounters := inst.buildMetrics(&parsedRsp.Memory.Counters, true)
	memGauges := inst.buildMetrics(&parsedRsp.Memory.Gauges, false)
	results = append(results, memCounters...)
	results = append(results, memGauges...)
	for handler, metrics := range parsedRsp.Handlers {
		handlerCounters := inst.buildMetrics(&metrics.Counters, true)
		handlerGauges := inst.buildMetrics(&metrics.Gauges, false)
		appendHandlerDim(&handlerCounters, handler)
		appendHandlerDim(&handlerGauges, handler)

		results = append(results, handlerCounters...)
		results = append(results, handlerGauges...)
	}

	return results, nil
}
