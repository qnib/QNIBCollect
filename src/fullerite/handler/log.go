package handler

import (
	"fullerite/metric"

	"encoding/json"
	"fmt"
	"time"

	l "github.com/Sirupsen/logrus"
)

// Log type
type Log struct {
	BaseHandler
}

// NewLog returns a new Debug handler.
func NewLog(
	channel chan metric.Metric,
	initialInterval int,
	initialBufferSize int,
	initialBufferFlushInterval time.Duration,
	log *l.Entry) *Log {

	inst := new(Log)
	inst.name = "Log"

	inst.interval = initialInterval
	inst.maxBufferSize = initialBufferSize
	inst.bufferFlushInterval = initialBufferFlushInterval
	inst.log = log
	inst.channel = channel

	return inst
}

// Configure accepts the different configuration options for the Log handler
func (h *Log) Configure(configMap map[string]interface{}) {
	h.configureCommonParams(configMap)
}

// Run runs the handler main loop
func (h *Log) Run() {
	h.run(h.emitMetrics)
}

func (h *Log) convertToLog(incomingMetric metric.Metric) (string, error) {
	jsonOut, err := json.Marshal(incomingMetric)
	return string(jsonOut), err
}

func (h *Log) emitMetrics(metrics []metric.Metric) bool {
	h.log.Info("Starting to emit ", len(metrics), " metrics")

	if len(metrics) == 0 {
		h.log.Warn("Skipping send because of an empty payload")
		return false
	}

	for _, m := range metrics {
		if dpString, err := h.convertToLog(m); err != nil {
			h.log.Error(fmt.Sprintf("Cannot convert metric %q to JSON: %s", m, err))
			continue
		} else {
			h.log.Info(dpString)
		}
	}
	return true
}
