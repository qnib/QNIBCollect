package handler

import (
	"fmt"
	"time"

	"fullerite/metric"
	l "github.com/Sirupsen/logrus"
	zmq "github.com/pebbe/zmq4"
)

func init() {
	RegisterHandler("ZmqPUB", newZmqPUB)
}

// ZmqPUB implements a simple way of reusing http connections
type ZmqPUB struct {
	BaseHandler
	port   string
	socket *zmq.Socket
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
		h.socket, err = zmq.NewSocket(zmq.PUB)
		if err != nil {
			msg := fmt.Sprintf("Could not create new PUB socket on '%s'", addr)
			h.log.Error(msg)
		} else {
			msg := fmt.Sprintf("Created new PUB socket on '%s'", addr)
			h.log.Info(msg)
			h.socket.Bind(addr)
		}
	} else {
		h.log.Info("Reuse existing socket")
	}
	h.configureCommonParams(configMap)
}

// Run runs the handler main loop
func (h *ZmqPUB) Run() {
	h.run(h.emitMetrics)
}

func (h *ZmqPUB) emitMetrics(metrics []metric.Metric) bool {
	h.log.Info("Starting to emit ", len(metrics), " metrics")
	for _, m := range metrics {
		h.socket.Send(m.ToJSON(), zmq.DONTWAIT)
	}
	return true
}
