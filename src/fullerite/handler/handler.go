package handler

import (
	"fullerite/config"
	"fullerite/metric"

	"container/list"
	"fmt"
	"time"

	l "github.com/Sirupsen/logrus"
)

// Some sane values to default things to
const (
	DefaultBufferSize          = 100
	DefaultBufferFlushInterval = 1
	DefaultInterval            = 10
	DefaultTimeoutSec          = 2

	MaxInt = 1<<32 - 1
)

var defaultLog = l.WithFields(l.Fields{"app": "fullerite", "pkg": "handler"})

// New creates a new Handler based on the requested handler name.
func New(name string) Handler {
	var base Handler

	channel := make(chan metric.Metric)
	handlerLog := defaultLog.WithFields(l.Fields{"handler": name})
	timeout := time.Duration(DefaultTimeoutSec * time.Second)
	defaultBufferFlushInterval := time.Duration(DefaultBufferFlushInterval) * time.Second

	switch name {
	case "Graphite":
		base = NewGraphite(channel, DefaultInterval, DefaultBufferSize, defaultBufferFlushInterval, timeout, handlerLog)
	case "SignalFx":
		base = NewSignalFx(channel, DefaultInterval, DefaultBufferSize, defaultBufferFlushInterval, timeout, handlerLog)
	case "Datadog":
		base = NewDatadog(channel, DefaultInterval, DefaultBufferSize, defaultBufferFlushInterval, timeout, handlerLog)
	case "Kairos":
		base = NewKairos(channel, DefaultInterval, DefaultBufferSize, defaultBufferFlushInterval, timeout, handlerLog)
	case "Log":
		base = NewLog(channel, DefaultInterval, DefaultBufferSize, defaultBufferFlushInterval, handlerLog)
	default:
		defaultLog.Error("Cannot create handler ", name)
		return nil
	}
	return base
}

// InternalMetrics holds the key:value pairs for counters/gauges
type InternalMetrics struct {
	Counters map[string]float64
	Gauges   map[string]float64
}

// NewInternalMetrics initializes the internal components of InternalMetrics
func NewInternalMetrics() *InternalMetrics {
	inst := new(InternalMetrics)
	inst.Counters = make(map[string]float64)
	inst.Gauges = make(map[string]float64)
	return inst
}

// Handler defines the interface of a generic handler.
type Handler interface {
	Run()
	Configure(map[string]interface{})

	// InternalMetrics is to publish a set of values
	// that are relevant to the handler itself.
	InternalMetrics() InternalMetrics

	// taken care of by the base
	Name() string
	String() string
	Channel() chan metric.Metric

	Interval() int
	SetInterval(int)

	BufferFlushInterval() time.Duration

	MaxBufferSize() int
	SetMaxBufferSize(int)

	Prefix() string
	SetPrefix(string)

	DefaultDimensions() map[string]string
	SetDefaultDimensions(map[string]string)
}

type emissionTiming struct {
	timestamp   time.Time
	duration    time.Duration
	metricsSent int
}

// BaseHandler is class to handle the boiler plate parts of the handlers
type BaseHandler struct {
	channel           chan metric.Metric
	name              string
	prefix            string
	defaultDimensions map[string]string
	log               *l.Entry

	interval            int
	bufferFlushInterval time.Duration
	maxBufferSize       int
	timeout             time.Duration

	// for tracking
	emissionTimes  list.List
	totalEmissions uint64
	metricsSent    uint64
	metricsDropped uint64
}

// SetMaxBufferSize : set the buffer size
func (base *BaseHandler) SetMaxBufferSize(size int) {
	base.maxBufferSize = size
}

// SetInterval : set the interval
func (base *BaseHandler) SetInterval(val int) {
	base.interval = val
}

// SetBufferFlushInterval : set the buffer flush interval
func (base *BaseHandler) SetBufferFlushInterval(val int) {
	base.bufferFlushInterval = time.Duration(val) * time.Second
}

// SetPrefix : any prefix that should be applied to the metrics name as they're sent
// it is appended without any punctuation, include your own
func (base *BaseHandler) SetPrefix(prefix string) {
	base.prefix = prefix
}

// SetDefaultDimensions : set the defautl dimensions
func (base *BaseHandler) SetDefaultDimensions(defaults map[string]string) {
	base.defaultDimensions = make(map[string]string)
	for name, value := range defaults {
		base.defaultDimensions[name] = value
	}
}

// Channel : the channel to handler listens for metrics on
func (base BaseHandler) Channel() chan metric.Metric {
	return base.channel
}

// Name : the name of the handler
func (base BaseHandler) Name() string {
	return base.name
}

// MaxBufferSize : the maximum number of metrics that should be buffered before sending
func (base BaseHandler) MaxBufferSize() int {
	return base.maxBufferSize
}

// BufferFlushInterval : the maximum interval of time to wait before flushing buffer
func (base BaseHandler) BufferFlushInterval() time.Duration {
	return base.bufferFlushInterval
}

// Prefix : the prefix (with punctuation) to use on each emitted metric
func (base BaseHandler) Prefix() string {
	return base.prefix
}

// DefaultDimensions : dimensions that should be included in any metric
func (base BaseHandler) DefaultDimensions() map[string]string {
	return base.defaultDimensions
}

// Interval : the maximum interval that the handler should buffer stats for
func (base BaseHandler) Interval() int {
	return base.interval
}

// String returns the handler name in a printable format.
func (base BaseHandler) String() string {
	return base.name + "Handler"
}

// InternalMetrics : Returns the internal metrics that are being collected by this handler
func (base BaseHandler) InternalMetrics() InternalMetrics {
	counters := map[string]float64{
		"totalEmissions": float64(base.totalEmissions),
		"metricsDropped": float64(base.metricsDropped),
		"metricsSent":    float64(base.metricsSent),
	}
	gauges := map[string]float64{
		"intervalLength":    float64(base.interval),
		"emissionsInWindow": float64(base.emissionTimes.Len()),
	}

	// now we calculate the average emission seconds for
	if base.emissionTimes.Len() > 0 {
		avg := 0.0
		max := 0.0

		var totalTime float64
		for e := base.emissionTimes.Front(); e != nil; e = e.Next() {
			dur := e.Value.(emissionTiming).duration.Seconds()
			totalTime += dur
			if dur > max {
				max = dur
			}
		}
		avg = totalTime / float64(base.emissionTimes.Len())
		gauges["averageEmissionTiming"] = avg
		gauges["maxEmissionTiming"] = max
	}

	return InternalMetrics{
		Counters: counters,
		Gauges:   gauges,
	}
}

// configureCommonParams will extract the common parameters that are used and set them in the handler
func (base *BaseHandler) configureCommonParams(configMap map[string]interface{}) {
	if asInterface, exists := configMap["timeout"]; exists {
		timeout := config.GetAsFloat(asInterface, DefaultTimeoutSec)
		base.timeout = time.Duration(timeout) * time.Second
	}

	if asInterface, exists := configMap["max_buffer_size"]; exists {
		base.maxBufferSize = config.GetAsInt(asInterface, DefaultBufferSize)
	}

	if asInterface, exists := configMap["buffer_flush_interval"]; exists {
		bufferFlushInterval := config.GetAsInt(asInterface, DefaultBufferFlushInterval)
		if bufferFlushInterval > 0 {
			base.bufferFlushInterval = time.Duration(bufferFlushInterval) * time.Second
		} else {
			base.bufferFlushInterval = time.Duration(MaxInt) * time.Second
		}
	}

	if asInterface, exists := configMap["interval"]; exists {
		base.interval = config.GetAsInt(asInterface, DefaultInterval)
	}

	// Default dimensions can be extended or overridden on a per handler basis.
	if asInterface, exists := configMap["defaultDimensions"]; exists {
		handlerLevelDimensions := config.GetAsMap(asInterface)
		base.SetDefaultDimensions(handlerLevelDimensions)
	}
}

func (base *BaseHandler) run(emitFunc func([]metric.Metric) bool) {
	metrics := make([]metric.Metric, 0, base.maxBufferSize)

	lastEmission := time.Now()
	emissionResults := make(chan emissionTiming)

	flusher := time.NewTicker(base.bufferFlushInterval).C

	go base.recordEmissions(emissionResults)
	for {
		select {
		case incomingMetric := <-base.Channel():
			base.log.Debug(base.name, " metric: ", incomingMetric)
			metrics = append(metrics, incomingMetric)

			emitIntervalPassed := time.Since(lastEmission).Seconds() >= float64(base.interval)
			bufferSizeLimitReached := len(metrics) >= base.maxBufferSize

			if emitIntervalPassed || bufferSizeLimitReached {
				go base.emitAndTime(metrics, emitFunc, emissionResults)

				// will get copied into this call, meaning it's ok to clear it
				metrics = nil
				lastEmission = time.Now()
			}
		case <-flusher:
			if len(metrics) > 0 {
				go base.emitAndTime(metrics, emitFunc, emissionResults)
				metrics = nil
			}
		}
	}
}

// manages the rolling window of emissions
// the emissions are a timesorted list, and we purge things older than
// the base handler's interval
func (base *BaseHandler) recordEmissions(timingsChannel chan emissionTiming) {
	for timing := range timingsChannel {
		base.totalEmissions++
		now := time.Now()

		base.emissionTimes.PushBack(timing)

		// now kull the list of old times, iterate through the list until we find
		// a timestamp that is within the interval
		minTime := now.Add(time.Duration(-1*base.interval) * time.Second)
		toRemove := []*list.Element{}
		for e := base.emissionTimes.Front(); e != nil && minTime.After(e.Value.(emissionTiming).timestamp); e = e.Next() {
			toRemove = append(toRemove, e)
		}

		for _, entry := range toRemove {
			base.emissionTimes.Remove(entry)
		}
		base.log.Debug("We removed ", len(toRemove), " entries and now have ", base.emissionTimes.Len())
	}
}

func (base *BaseHandler) emitAndTime(
	metrics []metric.Metric,
	emitFunc func([]metric.Metric) bool,
	callbackChannel chan emissionTiming,
) {
	numMetrics := len(metrics)
	beforeEmission := time.Now()
	result := emitFunc(metrics)
	afterEmission := time.Now()

	emissionDuration := afterEmission.Sub(beforeEmission)
	timing := emissionTiming{
		timestamp:   time.Now(),
		duration:    emissionDuration,
		metricsSent: numMetrics,
	}
	base.log.Info(
		fmt.Sprintf("POST of %d metrics to %s took %f seconds",
			numMetrics,
			base.name,
			emissionDuration.Seconds(),
		),
	)
	callbackChannel <- timing

	if result {
		base.metricsSent += uint64(numMetrics)
	} else {
		base.metricsDropped += uint64(numMetrics)
	}
}
