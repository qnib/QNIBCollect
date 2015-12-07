package collector

import (
	"fullerite/metric"
	"path"
	"test_utils"

	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCpuInfoCollect(t *testing.T) {
	config := make(map[string]interface{})
	config["procPath"] = path.Join(test_utils.DirectoryOfCurrentFile(), "/../../fixtures/proc/cpuinfo")
	testChannel := make(chan metric.Metric)
	testLogger := test_utils.BuildLogger()

	cpuInfo := NewCPUInfo(testChannel, 100, testLogger)
	cpuInfo.Configure(config)

	go cpuInfo.Collect()

	select {
	case m := <-cpuInfo.Channel():
		assert.Equal(t, 2.0, m.Value)
		assert.Equal(t, "Xeon(R) CPU E5-2630 0 @ 2.30GHz", m.Dimensions["model"])
		return
	case <-time.After(2 * time.Second):
		t.Fail()
	}
}
