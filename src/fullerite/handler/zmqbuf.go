package handler

import (
	"fmt"
	"strconv"
	"time"

	"fullerite/metric"
	l "github.com/Sirupsen/logrus"
	zmq "github.com/pebbe/zmq4"
	ring "mettring"
)

func init() {
	RegisterHandler("ZmqBUF", newZmqBUF)
}

// ZmqBUF implements a simple way of reusing http connections
type ZmqBUF struct {
	BaseHandler
	port          string
	retention     string
	sweepinterval string
	socket        *zmq.Socket
	buffer        ring.Ring
}

// Port returns the server's port number
func (h ZmqBUF) Port() string {
	return h.port
}

// Retention returns the rings retention time (in nanosec)
func (h ZmqBUF) Retention() int {
	i, _ := strconv.Atoi(h.retention)
	return i
}

// SweepInterval returns the rings retention time (in nanosec)
func (h ZmqBUF) SweepInterval() time.Duration {
	i, _ := strconv.Atoi(h.sweepinterval)
	return time.Second * time.Duration(i)
}

// newZmqBUF returns a new handler.
func newZmqBUF(
	channel chan metric.Metric,
	initialInterval int,
	initialBufferSize int,
	initialTimeout time.Duration,
	log *l.Entry) Handler {
	inst := new(ZmqBUF)
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
	if retention, exists := configMap["retention"]; exists {
		h.retention = retention.(string)
	} else {
		h.log.Error("There was no retention specified for the ZmqBUF Handler, there won't be any emissions")
	}
	if sweep, exists := configMap["sweepinterval"]; exists {
		h.sweepinterval = sweep.(string)
	} else {
		h.log.Warn("There was no sweep interval specified for the ZmqBUF Handler, use 5 (sec)")
		h.sweepinterval = "5"
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
	//Initialize ring-buffer
	h.buffer = ring.New(h.Retention())
	h.configureCommonParams(configMap)
}

func (h *ZmqBUF) serveReq() {
	for {
		msg, _ := h.socket.Recv(0)
		h.log.Info("Received request: ", msg)
		reply, _ := h.Values()
		for _, rep := range reply {
			h.socket.Send(rep.ToJSON(), zmq.SNDMORE)
		}
		h.socket.Send("EOM", 0)

	}
}

// Run runs the handler main loop
func (h *ZmqBUF) Run() {
	go h.serveReq()
	go h.sweepTicker()
	h.run(h.emitMetrics)
}

// Enqueue puts metric into buffer
func (h *ZmqBUF) Enqueue(m metric.Metric) {
	if m.Buffered {
		h.buffer.Enqueue(m)
	}
}

// sweepTicker ticks every sweepinterval and remove old entries
func (h *ZmqBUF) sweepTicker() {
	ticker := time.NewTicker(h.SweepInterval())
	for t := range ticker.C {
		cnt, ok := h.buffer.TidyUp()
		if !ok {
			h.log.Warn("TidyUp went wrong... :(")
		} else {
			h.log.Info(fmt.Sprintf("%s Kicked out %d metrics during TidyUp()", t, cnt))
		}
	}
}

// Values returns all Values in the buffer
func (h *ZmqBUF) Values() ([]metric.Metric, bool) {
	return h.buffer.Values()
}

// Filter returns Values in the buffer which do not match the metric.Filter
func (h *ZmqBUF) Filter(f metric.Filter) ([]metric.Metric, bool) {
	slice, ok := h.buffer.Filter(f)
	//h.log.Debug("Filter result length: ", len(slice))
	return slice, ok
}

// Match returns Values in the buffer which match the metric.Filter
func (h *ZmqBUF) Match(f metric.Filter) ([]metric.Metric, bool) {
	slice, ok := h.buffer.Match(f)
	//h.log.Debug("Filter result length: ", len(slice))
	return slice, ok
}

func (h *ZmqBUF) emitMetrics(metrics []metric.Metric) bool {
	h.log.Info("Starting to emit ", len(metrics), " metrics")
	for _, m := range metrics {
		h.Enqueue(m)
	}
	return true
}
