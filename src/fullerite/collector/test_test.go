package collector

import (
	"fullerite/metric"
	"test_utils"

	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTestConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})
	test := newTest(nil, 123, nil)
	test.Configure(config)

	assert.Equal(t,
		test.Interval(),
		123,
		"should be the default collection interval",
	)
}

func TestTestConfigure(t *testing.T) {
	config := make(map[string]interface{})
	config["interval"] = 9999

	// the channel and logger don't matter
	test := newTest(nil, 12, nil)
	test.Configure(config)

	assert.Equal(t,
		test.Interval(),
		9999,
		"should be the defined interval",
	)
}

func TestTestConfigureMetricName(t *testing.T) {
	config := make(map[string]interface{})
	config["metricName"] = "lala"

	testChannel := make(chan metric.Metric)
	testLogger := test_utils.BuildLogger()

	test := newTest(testChannel, 123, testLogger)
	test.Configure(config)

	go test.Collect()

	select {
	case m := <-test.Channel():
		// don't test for the value - only metric name
		assert.Equal(t, m.Name, "lala")
	case <-time.After(4 * time.Second):
		t.Fail()
	}
}

func TestTestCollect(t *testing.T) {
	config := make(map[string]interface{})

	testChannel := make(chan metric.Metric)
	testLogger := test_utils.BuildLogger()

	// conforms to the valueGenerator interface in the collector
	mockGen := func() float64 {
		return 4.0
	}

	test := newTest(testChannel, 123, testLogger)
	test.Configure(config)
	test.generator = mockGen

	go test.Collect()

	select {
	case m := <-test.Channel():
		assert.Equal(t, 4.0, m.Value)
		return
	case <-time.After(4 * time.Second):
		t.Fail()
	}
}
