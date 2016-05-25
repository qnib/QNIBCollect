package handler

import (
	"fmt"
	"time"

	"fullerite/metric"
	ring "github.com/ChristianKniep/go-mettring"
	l "github.com/Sirupsen/logrus"
	zmq "github.com/pebbe/zmq4"
)

func init() {
	RegisterHandler("ZmqBUF", newZmqBUF)
}

// ZmqBUF implements a simple way of reusing http connections
type ZmqBUF struct {
	BaseHandler
	port     string
	capacity int
	socket   *zmq.Socket
	buffer   ring.Ring
}

// Port returns the server's port number
func (h ZmqBUF) Port() string {
	return h.port
}

// newZmqPUB returns a new handler.
func newZmqBUF(
	channel chan metric.Metric,
	initialInterval int,
	initialBufferSize int,
	initialTimeout time.Duration,
	log *l.Entry) Handler {
	inst := new(ZmqPUB)
	inst.name = "ZmqBUF"
	inst.interval = initialInterval
	inst.maxBufferSize = initialBufferSize
	inst.timeout = initialTimeout
	inst.log = log
	inst.channel = channel

	return inst
}

// Configure accepts the different configuration options for the InfluxDB handler
func (h *ZmqBUF) Configure(configMap map[string]interface{}) {
	if port, exists := configMap["port"]; exists {
		h.port = port.(string)
	} else {
		h.log.Error("There was no port specified for the ZmqBUF Handler, there won't be any emissions")
	}
	if capacity, exists := configMap["capacity"]; exists {
		h.capacity = capacity.(int)
	} else {
		h.log.Error("There was no capacity specified for the ZmqBUF Handler, there won't be any emissions")
	}

	// Create connection if not existing
	if h.socket == nil {
		addr := fmt.Sprintf("tcp://*:%s", h.port)
		var err error
		h.socket, err = zmq.NewSocket(zmq.REP)
		if err != nil {
			msg := fmt.Sprintf("Could not create new REP socket on '%s'", addr)
			h.log.Error(msg)
		} else {
			msg := fmt.Sprintf("Created new REP socket on '%s'", addr)
			h.log.Info(msg)
			h.socket.Bind(addr)
		}
	} else {
		h.log.Info("Reuse existing socket")
	}
	//Initialize ring-buffer with 300.000ms (300s -> 5m)
	h.buffer = ring.New(300000)
	h.configureCommonParams(configMap)
}

// Run runs the handler main loop
func (h *ZmqBUF) Run() {
	h.run(h.emitMetrics)
}

func (h *ZmqBUF) emitMetrics(metrics []metric.Metric) bool {
	h.log.Info("Starting to emit ", len(metrics), " metrics")
	for _, m := range metrics {
		h.buffer.Enqueue(m)
	}
	return true
}
