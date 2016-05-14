package collector

import (
	"fullerite/metric"
	"test_utils"

	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFulleriteConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})

	f := newFullerite(nil, 123, nil)
	f.Configure(config)

	assert.Equal(t,
		f.Interval(),
		123,
		"should be the default collection interval",
	)
}

func TestFulleriteConfigure(t *testing.T) {
	config := make(map[string]interface{})
	config["interval"] = 9999

	f := newFullerite(nil, 123, nil)
	f.Configure(config)

	assert.Equal(t,
		f.Interval(),
		9999,
		"should be the defined interval",
	)
}

func TestFulleriteCollect(t *testing.T) {
	config := make(map[string]interface{})

	testChannel := make(chan metric.Metric)
	testLog := test_utils.BuildLogger()

	f := newFullerite(testChannel, 123, testLog)
	f.Configure(config)

	go f.Collect()

	select {
	case <-f.Channel():
		return
	case <-time.After(2 * time.Second):
		t.Fail()
	}
}
