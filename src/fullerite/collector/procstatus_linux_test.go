// +build linux

package collector

import (
	"fullerite/metric"
	"test_utils"

	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProcStatusCollect(t *testing.T) {
	config := make(map[string]interface{})
	config["interval"] = 9999

	dims := map[string]string{
		"module": "(.*)",
	}

	config["generatedDimensions"] = dims

	channel := make(chan metric.Metric)

	testLog := test_utils.BuildLogger()
	ps := newProcStatus(channel, 12, testLog)
	ps.Configure(config)

	go ps.Collect()

	select {
	case <-ps.Channel():
		return
	case <-time.After(2 * time.Second):
		t.Fail()
	}
}

func TestProcStatusCollectMetricTypes(t *testing.T) {
	config := make(map[string]interface{})
	config["interval"] = 9999

	dims := map[string]string{
		"module": "(.*)",
	}

	config["generatedDimensions"] = dims

	channel := make(chan metric.Metric)

	testLog := test_utils.BuildLogger()
	ps := newProcStatus(channel, 12, testLog)
	ps.Configure(config)

	go ps.Collect()

	select {
	case <-ps.Channel():
		for _, m := range ps.procStatusMetrics() {
			if m.Name == "CPUTime" {
				assert.Equal(t, m.MetricType, metric.CumulativeCounter, "CPUTime is a CumulativeCounter")
			} else {
				assert.Equal(t, m.MetricType, metric.Gauge, "All others are a Gauge")
			}
		}
	}
}

func TestProcStatusExtractDimensions(t *testing.T) {
	testLog := test_utils.BuildLogger()

	config := make(map[string]interface{})

	dims := map[string]string{
		"module": "^python.*?test.*?\\.([^\\.]*)?\\-\\[\\d+\\]$",
		"order":  "^python.*?test.*?\\.[^\\.]*?\\-\\[(\\d+)\\]$",
	}
	config["generatedDimensions"] = dims

	ps := newProcStatus(nil, 12, testLog)
	ps.Configure(config)

	dim := map[string]string{
		"module": "bond",
		"order":  "007",
	}

	extracted := ps.extractDimensions("python -m test.my.function.bond-[007]")
	assert.Equal(t, dim, extracted)
}

func TestProcStatusMetrics(t *testing.T) {
	testLog := test_utils.BuildLogger()

	config := make(map[string]interface{})

	dims := map[string]string{
		"seven":  "(.......)",
		"eleven": "(...........)",
	}
	config["generatedDimensions"] = dims

	ps := newProcStatus(nil, 12, testLog)
	ps.Configure(config)

	count := 0
	for _, m := range ps.procStatusMetrics() {
		mDims := m.Dimensions
		_, existsSeven := mDims["seven"]
		_, existsEleven := mDims["eleven"]
		if existsSeven == false || existsEleven == false {
			continue
		}
		count++
	}
	if count == 0 {
		t.Fail()
	}
}

func TestProcStatusMatches(t *testing.T) {
	assert := assert.New(t)
	testLog := test_utils.BuildLogger()
	ps := newProcStatus(nil, 12, testLog)
	config := make(map[string]interface{})

	commGenerator := func(comm string, err error) func() (string, error) {
		return func() (string, error) {
			return comm, err
		}
	}

	config["pattern"] = ".*"
	config["matchCommandLine"] = true
	ps.Configure(config)

	match := ps.matches([]string{"proc", "status"}, commGenerator("proc", nil))
	assert.True(match)
	match = ps.matches([]string{"proc", "status"}, commGenerator("proc", errors.New("")))
	assert.True(match)

	config["pattern"] = ".*"
	config["matchCommandLine"] = false
	ps.Configure(config)

	match = ps.matches([]string{"proc", "status"}, commGenerator("proc", nil))
	assert.True(match)
	match = ps.matches([]string{"proc", "status"}, commGenerator("proc", errors.New("")))
	assert.False(match)

	config["pattern"] = "sta"
	config["matchCommandLine"] = true
	ps.Configure(config)

	match = ps.matches([]string{"proc", "status"}, commGenerator("proc", nil))
	assert.True(match)
	match = ps.matches([]string{"proc", "butter"}, commGenerator("proc", nil))
	assert.False(match)
	match = ps.matches([]string{"proc", "butter"}, commGenerator("status", nil))
	assert.False(match)
	match = ps.matches([]string{"proc", "status"}, commGenerator("proc", errors.New("")))
	assert.True(match)

	config["pattern"] = "pro"
	config["matchCommandLine"] = false
	ps.Configure(config)

	match = ps.matches([]string{"proc", "status"}, commGenerator("proc", nil))
	assert.True(match)
	match = ps.matches([]string{"peanut", "status"}, commGenerator("peanut", nil))
	assert.False(match)
	match = ps.matches([]string{"proc", "status"}, commGenerator("peanut", nil))
	assert.False(match)
	match = ps.matches([]string{"proc", "status"}, commGenerator("proc", errors.New("")))
	assert.False(match)

	config["pattern"] = "oc sta"
	config["matchCommandLine"] = true
	ps.Configure(config)

	match = ps.matches([]string{"proc", "status"}, commGenerator("proc", nil))
	assert.True(match)
}
