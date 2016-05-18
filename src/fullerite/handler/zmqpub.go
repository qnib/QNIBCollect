package handler

import (
	"fmt"
	"time"

	"fullerite/metric"
	l "github.com/Sirupsen/logrus"
	goczmq "github.com/zeromq/goczmq"
)

func init() {
	RegisterHandler("ZmqPUB", newZmqPUB)
}

// ZmqPUB implements a simple way of reusing http connections
type ZmqPUB struct {
	BaseHandler
	port   string
	socket *goczmq.Sock
}

// Port returns the server's port number
func (h ZmqPUB) Port() string {
	return h.port
}

// newZmqPUB returns a new handler.
func newZmqPUB(
	channel chan metric.Metric,
	initialInterval int,
	initialBufferSize int,
	initialTimeout time.Duration,
	log *l.Entry) Handler {
	inst := new(ZmqPUB)
	inst.name = "ZmqPUB"
	inst.interval = initialInterval
	inst.maxBufferSize = initialBufferSize
	inst.timeout = initialTimeout
	inst.log = log
	inst.channel = channel

	return inst
}

// Configure accepts the different configuration options for the InfluxDB handler
func (h *ZmqPUB) Configure(configMap map[string]interface{}) {
	if port, exists := configMap["port"]; exists {
		h.port = port.(string)
	} else {
		h.log.Error("There was no port specified for the ZMQPUB Handler, there won't be any emissions")
	}

	// Create connection if not existing
	if h.socket == nil {
		addr := fmt.Sprintf("tcp://*:%s", h.port)
		var err error
		h.socket, err = goczmq.NewPub(addr)
		if err != nil {
			msg := fmt.Sprintf("Could not create new RADIO socket on '%s'", addr)
			h.log.Error(msg)
		} else {
			h.log.Info("Reuse existing socket")
		}
	}
	h.configureCommonParams(configMap)
}

// Run runs the handler main loop
func (h *ZmqPUB) Run() {
	h.run(h.emitMetrics)
}

func (h *ZmqPUB) emitMetrics(metrics []metric.Metric) bool {
	h.log.Info("Starting to emit ", len(metrics), " metrics")

	return true
}
