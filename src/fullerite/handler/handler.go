package handler

import (
	"fullerite/config"
	"fullerite/metric"
	"sync/atomic"

	"container/list"
	"fmt"
	"time"

	l "github.com/Sirupsen/logrus"
)

// Some sane values to default things to
const (
	DefaultBufferSize                = 100
	DefaultInterval                  = 10
	DefaultTimeoutSec                = 2
	DefaultMaxIdleConnectionsPerHost = 2
	DefaultKeepAliveInterval         = 30
)

var defaultLog = l.WithFields(l.Fields{"app": "fullerite", "pkg": "handler"})

// New creates a new Handler based on the requested handler name.
func New(name string) Handler {
	var base Handler

	channel := make(chan metric.Metric)
	handlerLog := defaultLog.WithFields(l.Fields{"handler": name})
	timeout := time.Duration(DefaultTimeoutSec * time.Second)

	switch name {
	case "Graphite":
		base = NewGraphite(channel, DefaultInterval, DefaultBufferSize, timeout, handlerLog)
	case "SignalFx":
		base = NewSignalFx(channel, DefaultInterval, DefaultBufferSize, timeout, handlerLog)
	case "Datadog":
		base = NewDatadog(channel, DefaultInterval, DefaultBufferSize, timeout, handlerLog)
	case "Kairos":
		base = NewKairos(channel, DefaultInterval, DefaultBufferSize, timeout, handlerLog)
	case "Log":
		base = NewLog(channel, DefaultInterval, DefaultBufferSize, handlerLog)
	case "Scribe":
		base = NewScribe(channel, DefaultInterval, DefaultBufferSize, timeout, handlerLog)
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
	InitListeners(config.Config)

	// InternalMetrics is to publish a set of values
	// that are relevant to the handler itself.
	InternalMetrics() InternalMetrics

	// taken care of by the base
	Name() string
	String() string
	Channel() chan metric.Metric

	CollectorChannels() map[string]chan metric.Metric
	SetCollectorChannels(map[string]chan metric.Metric)

	Interval() int
	SetInterval(int)

	MaxBufferSize() int
	SetMaxBufferSize(int)

	Prefix() string
	SetPrefix(string)

	DefaultDimensions() map[string]string
	SetDefaultDimensions(map[string]string)

	MaxIdleConnectionsPerHost() int
	SetMaxIdleConnectionsPerHost(int)

	KeepAliveInterval() int
	SetKeepAliveInterval(int)

	// Return true if collector
	// is blacklisted in the handler
	SetCollectorBlackList([]string)
	CollectorBlackList() map[string]bool
	IsCollectorBlackListed(string) (bool, bool)

	// Return true if collector
	// is whitelisted in the handler
	SetCollectorWhiteList([]string)
	CollectorWhiteList() map[string]bool
	IsCollectorWhiteListed(string) (bool, bool)
}

type emissionTiming struct {
	timestamp   time.Time
	duration    time.Duration
	metricsSent int
}

// BaseHandler is class to handle the boiler plate parts of the handlers
type BaseHandler struct {
	channel           chan metric.Metric
	collectorChannels map[string]chan metric.Metric
	name              string
	prefix            string
	defaultDimensions map[string]string
	log               *l.Entry

	interval      int
	maxBufferSize int
	timeout       time.Duration

	// for keepalive
	maxIdleConnectionsPerHost int
	keepAliveInterval         int

	// for tracking
	emissionTimes  list.List
	totalEmissions uint64
	metricsSent    uint64
	metricsDropped uint64

	// List of blacklisted collectors
	// the handler won't accept metrics from
	blackListedCollectors map[string]bool

	// List of whitelisted collectors
	// the handler will accept metrics from
	whiteListedCollectors map[string]bool
}

// SetMaxBufferSize : set the buffer size
func (base *BaseHandler) SetMaxBufferSize(size int) {
	base.maxBufferSize = size
}

// SetInterval : set the interval
func (base *BaseHandler) SetInterval(val int) {
	base.interval = val
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

// CollectorChannels : the channels to handler listens for metrics on
func (base BaseHandler) CollectorChannels() map[string]chan metric.Metric {
	return base.collectorChannels
}

// SetCollectorChannels : the channels to handler listens for metrics on
func (base *BaseHandler) SetCollectorChannels(c map[string]chan metric.Metric) {
	base.collectorChannels = make(map[string]chan metric.Metric)
	for name, channel := range c {
		base.collectorChannels[name] = channel
	}
}

// Name : the name of the handler
func (base BaseHandler) Name() string {
	return base.name
}

// MaxBufferSize : the maximum number of metrics that should be buffered before sending
func (base BaseHandler) MaxBufferSize() int {
	return base.maxBufferSize
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

// SetMaxIdleConnectionsPerHost : Set maximum idle connections per host
func (base *BaseHandler) SetMaxIdleConnectionsPerHost(value int) {
	base.maxIdleConnectionsPerHost = value
}

// SetKeepAliveInterval : Set keep alive interval
func (base *BaseHandler) SetKeepAliveInterval(value int) {
	base.keepAliveInterval = value
}

// SetCollectorBlackList : Add collectors mentioned in the handler config to blacklist
func (base *BaseHandler) SetCollectorBlackList(blackList []string) {
	base.blackListedCollectors = make(map[string]bool)
	for _, collectorName := range blackList {
		base.blackListedCollectors[collectorName] = true
	}
}

// IsCollectorBlackListed : return true if collectorName is blacklisted in the handler
func (base *BaseHandler) IsCollectorBlackListed(collectorName string) (bool, bool) {
	val, exists := base.blackListedCollectors[collectorName]
	return val, exists
}

// CollectorBlackList : return handler specific black listed collectors
func (base *BaseHandler) CollectorBlackList() map[string]bool {
	return base.blackListedCollectors
}

// SetCollectorWhiteList : Add collectors mentioned in the handler config to the whitelist
func (base *BaseHandler) SetCollectorWhiteList(whiteList []string) {
	base.whiteListedCollectors = make(map[string]bool)
	for _, collectorName := range whiteList {
		base.whiteListedCollectors[collectorName] = true
	}
}

// IsCollectorWhiteListed : return true if collectorName is blacklisted in the handler
func (base *BaseHandler) IsCollectorWhiteListed(collectorName string) (bool, bool) {
	val, exists := base.whiteListedCollectors[collectorName]
	return val, exists
}

// CollectorWhiteList : return handler specific black listed collectors
func (base *BaseHandler) CollectorWhiteList() map[string]bool {
	return base.whiteListedCollectors
}

// MaxIdleConnectionsPerHost : return max idle connections per host
func (base *BaseHandler) MaxIdleConnectionsPerHost() int {
	return base.maxIdleConnectionsPerHost
}

// InitListeners - initiate listener channels for collectors
func (base *BaseHandler) InitListeners(globalConfig config.Config) {
	collectorChannels := make(map[string]chan metric.Metric)
	for _, c := range append(globalConfig.Collectors, globalConfig.DiamondCollectors...) {

		// If the handler's whitelist is set, then only metrics from collectors in it will be emitted. If the same
		// collector is also in the blacklist, it will be skipped.
		// If the handler's whitelist is not set and its blacklist is not empty, only metrics from collectors not in
		// the blacklist will be emitted.
		isWhiteListed, _ := base.IsCollectorWhiteListed(c)
		isBlackListed, _ := base.IsCollectorBlackListed(c)

		// If the handler's whitelist is not nil and not empty, only the whitelisted collectors should be considered
		if base.CollectorWhiteList() != nil && len(base.CollectorWhiteList()) > 0 {
			if !isWhiteListed || isBlackListed {
				continue
			}
		} else {
			// If the handler's whitelist is nil, all collector except the ones in the blacklist are enabled
			if isBlackListed {
				continue
			}
		}
		collectorChannels[c] = make(chan metric.Metric, 1)
	}
	base.SetCollectorChannels(collectorChannels)
}

// KeepAliveInterval - return keep alive interval
func (base BaseHandler) KeepAliveInterval() int {
	return base.keepAliveInterval
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

	if asInterface, exists := configMap["interval"]; exists {
		base.interval = config.GetAsInt(asInterface, DefaultInterval)
	}

	// Default dimensions can be extended or overridden on a per handler basis.
	if asInterface, exists := configMap["defaultDimensions"]; exists {
		handlerLevelDimensions := config.GetAsMap(asInterface)
		base.SetDefaultDimensions(handlerLevelDimensions)
	}

	if asInterface, exists := configMap["keepAliveInterval"]; exists {
		keepAliveInterval := config.GetAsInt(asInterface, DefaultKeepAliveInterval)
		base.SetKeepAliveInterval(keepAliveInterval)
	}

	if asInterface, exists := configMap["maxIdleConnectionsPerHost"]; exists {
		maxIdleConnectionsPerHost := config.GetAsInt(asInterface,
			DefaultMaxIdleConnectionsPerHost)
		base.SetMaxIdleConnectionsPerHost(maxIdleConnectionsPerHost)
	}

	if asInterface, exists := configMap["collectorBlackList"]; exists {
		blackList := config.GetAsSlice(asInterface)
		base.SetCollectorBlackList(blackList)
	}

	if asInterface, exists := configMap["collectorWhiteList"]; exists {
		whiteList := config.GetAsSlice(asInterface)
		base.SetCollectorWhiteList(whiteList)
	}
}

func (base *BaseHandler) run(emitFunc func([]metric.Metric) bool) {
	emissionResults := make(chan emissionTiming)
	go base.recordEmissions(emissionResults)

	for _, v := range base.CollectorChannels() {
		go base.listenForMetrics(emitFunc, v, emissionResults)
	}
	go base.listenForMetrics(emitFunc, base.Channel(), emissionResults)
}

func (base *BaseHandler) listenForMetrics(
	emitFunc func([]metric.Metric) bool,
	c chan metric.Metric,
	emissionResults chan emissionTiming) {
	metrics := make([]metric.Metric, 0, base.maxBufferSize)

	ticker := time.NewTicker(time.Duration(base.interval) * time.Second)
	flusher := ticker.C

	for {
		select {
		case incomingMetric := <-c:
			base.log.Debug(base.name, " metric: ", incomingMetric)
			metrics = append(metrics, incomingMetric)

			bufferSizeLimitReached := len(metrics) >= base.maxBufferSize

			if bufferSizeLimitReached {
				go base.emitAndTime(metrics, emitFunc, emissionResults)

				// will get copied into this call, meaning it's ok to clear it
				metrics = make([]metric.Metric, 0, base.maxBufferSize)
			}
		case <-flusher:
			if len(metrics) > 0 {
				go base.emitAndTime(metrics, emitFunc, emissionResults)
				metrics = make([]metric.Metric, 0, base.maxBufferSize)
			}
		}
	}
	ticker.Stop()
}

// manages the rolling window of emissions
// the emissions are a timesorted list, and we purge things older than
// the base handler's interval
func (base *BaseHandler) recordEmissions(timingsChannel chan emissionTiming) {
	for timing := range timingsChannel {
		atomic.AddUint64(&base.totalEmissions, 1)
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
		atomic.AddUint64(&base.metricsSent, uint64(numMetrics))
	} else {
		atomic.AddUint64(&base.metricsDropped, uint64(numMetrics))
	}
}
