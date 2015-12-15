package handler

import (
	"fullerite/metric"

	"fmt"
	"testing"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func assertEmpty(t *testing.T, channel chan metric.Metric) {
	close(channel)
	for range channel {
		t.Fatal("The channel was not empty")
	}
}

func TestNewHandler(t *testing.T) {
	names := []string{"Graphite", "Kairos", "SignalFx", "Datadog", "Log"}
	for _, name := range names {
		h := New(name)
		assert.NotNil(t, h, "should create a Handler for "+name)
		assert.NotNil(t, h.Channel(), "should create a channel")
		assert.Equal(t, name, h.Name())
		assert.Equal(t, "", h.Prefix(), "")
		assert.Equal(t, 0, len(h.DefaultDimensions()))
		assert.Equal(t, DefaultBufferSize, h.MaxBufferSize())
		assert.Equal(t, DefaultInterval, h.Interval())
		assert.Equal(t, name+"Handler", fmt.Sprintf("%s", h), "String() should append Handler to the name for "+name)

		// Test Set* functions
		h.SetInterval(999)
		assert.Equal(t, 999, h.Interval())

		h.SetMaxBufferSize(999)
		assert.Equal(t, 999, h.MaxBufferSize())

		dims := map[string]string{"test": "test value"}
		h.SetDefaultDimensions(dims)
		assert.Equal(t, 1, len(h.DefaultDimensions()))
	}
}

// If configured, per handler dimensions should over write default dimensions
func TestPerHandlerDimensions(t *testing.T) {
	b := new(BaseHandler)
	dims := map[string]string{"test": "test value", "host": "test host"}
	b.SetDefaultDimensions(dims)
	assert.Equal(t, 2, len(b.DefaultDimensions()))

	handlerLevelDimensions := "{ \"test\" : \"updated value\", \"runtimeenv\": \"dev\", \"region\":\"uswest1-devc\"}"
	configMap := map[string]interface{}{
		"defaultDimensions": handlerLevelDimensions,
	}

	b.configureCommonParams(configMap)
	assert.Equal(t, 3, len(b.DefaultDimensions()))
	assert.Equal(t, "updated value", b.DefaultDimensions()["test"])
	assert.Equal(t, "", b.DefaultDimensions()["host"])
}

func TestEmissionAndRecord(t *testing.T) {
	emitCalled := false

	callbackChannel := make(chan emissionTiming)
	emitFunc := func([]metric.Metric) bool {
		emitCalled = true
		return true
	}
	metrics := []metric.Metric{metric.New("example")}

	base := BaseHandler{}
	base.log = l.WithField("testing", "basehandler")
	go base.emitAndTime(metrics, emitFunc, callbackChannel)

	select {
	case timing := <-callbackChannel:
		assert.NotNil(t, timing)
		assert.Equal(t, 1, timing.metricsSent)
		assert.NotNil(t, timing.timestamp)
		assert.NotNil(t, timing.duration)
	case <-time.After(2 * time.Second):
		t.Fatal("Failed to read from the callback channel after 2 seconds")
	}

	assert.True(t, emitCalled)
}

func TestRecordTimings(t *testing.T) {
	base := BaseHandler{}
	base.log = l.WithField("testing", "basehandler")
	base.interval = 2

	minusFiveSec := -1 * 5 * time.Second
	minusSixSec := -1 * 6 * time.Second
	someDur := time.Duration(5)
	now := time.Now()

	// create a list of emissions in order with some older than 1 second
	timingsChannel := make(chan emissionTiming)
	base.emissionTimes.PushBack(emissionTiming{now.Add(minusSixSec), someDur, 0})
	base.emissionTimes.PushBack(emissionTiming{now.Add(minusFiveSec), someDur, 0})

	go base.recordEmissions(timingsChannel)
	timingsChannel <- emissionTiming{now, someDur, 0}

	assert.Equal(t, 1, base.emissionTimes.Len())
}

func TestHandlerRun(t *testing.T) {
	base := BaseHandler{}
	base.log = l.WithField("testing", "basehandler")
	base.interval = 1
	base.maxBufferSize = 1
	base.channel = make(chan metric.Metric)

	emitCalled := false
	emitFunc := func(metrics []metric.Metric) bool {
		assert.Equal(t, 1, len(metrics))
		emitCalled = true
		return true
	}

	// now we are waiting for some metrics
	go base.run(emitFunc)

	base.channel <- metric.New("testMetric")
	time.Sleep(1 * time.Second)
	assert.True(t, emitCalled)
	assert.Equal(t, 1, base.emissionTimes.Len())
	assert.Equal(t, uint64(1), base.metricsSent)
	assert.Equal(t, uint64(0), base.metricsDropped)
	assert.Equal(t, uint64(1), base.totalEmissions)
	assertEmpty(t, base.channel)
}

func TestInternalMetrics(t *testing.T) {
	base := BaseHandler{}
	base.totalEmissions = 10
	base.metricsDropped = 100
	base.metricsSent = 2
	base.interval = 4

	timing := emissionTiming{time.Now(), 5 * time.Second, 0}
	base.emissionTimes.PushBack(timing)
	timing = emissionTiming{time.Now(), 10 * time.Second, 0}
	base.emissionTimes.PushBack(timing)
	timing = emissionTiming{time.Now(), 6 * time.Second, 0}
	base.emissionTimes.PushBack(timing)

	results := base.InternalMetrics()
	expected := InternalMetrics{
		Counters: map[string]float64{
			"metricsDropped": 100,
			"metricsSent":    2,
			"totalEmissions": 10,
		},
		Gauges: map[string]float64{
			"averageEmissionTiming": 7,
			"emissionsInWindow":     3,
			"intervalLength":        4,
			"maxEmissionTiming":     10,
		},
	}
	assert.Equal(t, expected, results)
}

func TestInternalMetricsWithNan(t *testing.T) {
	base := BaseHandler{}

	expected := InternalMetrics{
		Counters: map[string]float64{
			"metricsDropped": 0,
			"metricsSent":    0,
			"totalEmissions": 0,
		},
		// specifically missing the averageEmissionTiming
		// because we have no emissions yet
		Gauges: map[string]float64{
			"emissionsInWindow": 0,
			"intervalLength":    0,
		},
	}
	im := base.InternalMetrics()
	assert.Equal(t, expected, im)
}
