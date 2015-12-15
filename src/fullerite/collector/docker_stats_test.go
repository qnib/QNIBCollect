package collector

import (
	"encoding/json"
	"fullerite/metric"

	"reflect"
	"testing"

	l "github.com/Sirupsen/logrus"
	"github.com/fsouza/go-dockerclient"
	"github.com/stretchr/testify/assert"
)

func getSUT() *DockerStats {
	expectedChan := make(chan metric.Metric)
	var expectedLogger = defaultLog.WithFields(l.Fields{"collector": "fullerite"})

	return NewDockerStats(expectedChan, 10, expectedLogger)
}

func TestDockerStatsNewDockerStats(t *testing.T) {
	expectedChan := make(chan metric.Metric)
	var expectedLogger = defaultLog.WithFields(l.Fields{"collector": "fullerite"})
	expectedType := make(map[string]*CPUValues)

	d := NewDockerStats(expectedChan, 10, expectedLogger)

	assert.Equal(t, d.log, expectedLogger)
	assert.Equal(t, d.channel, expectedChan)
	assert.Equal(t, d.interval, 10)
	assert.Equal(t, d.name, "DockerStats")
	assert.Equal(t, reflect.TypeOf(d.previousCPUValues), reflect.TypeOf(expectedType))
	assert.Equal(t, len(d.previousCPUValues), 0)
	assert.Equal(t, d.dockerClient.Endpoint(), endpoint)
}

func TestDockerStatsConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})

	d := NewDockerStats(nil, 123, nil)
	d.Configure(config)

	assert.Equal(t, 123, d.Interval())
}

func TestDockerStatsConfigure(t *testing.T) {
	config := make(map[string]interface{})
	config["interval"] = 9999

	d := NewDockerStats(nil, 123, nil)
	d.Configure(config)

	assert.Equal(t, 9999, d.Interval())
}

func TestDockerStatsBuildMetrics(t *testing.T) {
	stats := new(docker.Stats)
	stats.Network.RxBytes = 10
	stats.Network.TxBytes = 20
	stats.MemoryStats.Usage = 50
	stats.MemoryStats.Limit = 70

	containerJSON := []byte(`
	{
		"ID": "test-id",
		"Name": "test-container",
		"Config": {
			"Env": [
				"MESOS_TASK_ID=my--service.main.blablagit6bdsadnoise"
			]
		}
	}`)
	var container *docker.Container
	err := json.Unmarshal(containerJSON, &container)
	assert.Equal(t, err, nil)

	expectedDims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
		"instance_name":  "main",
		"collector":      "DockerStats",
	}
	expectedMetrics := []metric.Metric{
		metric.Metric{"DockerRxBytes", "cumcounter", 10, expectedDims},
		metric.Metric{"DockerTxBytes", "cumcounter", 20, expectedDims},
		metric.Metric{"DockerMemoryUsed", "gauge", 50, expectedDims},
		metric.Metric{"DockerMemoryLimit", "gauge", 70, expectedDims},
		metric.Metric{"DockerCpuPercentage", "gauge", 0.5, expectedDims},
	}

	d := getSUT()
	ret := d.buildMetrics(container, stats, 0.5)

	assert.Equal(t, ret, expectedMetrics)
}

func TestDockerStatsBuildMetricsWithNameAsEnvVariable(t *testing.T) {
	stats := new(docker.Stats)
	stats.Network.RxBytes = 10
	stats.Network.TxBytes = 20
	stats.MemoryStats.Usage = 50
	stats.MemoryStats.Limit = 70

	containerJSON := []byte(`
	{
		"ID": "test-id",
		"Name": "test-container",
		"Config": {
			"Env": [
				"SERVICE_NAME=my_service"
			]
		}
	}`)
	var container *docker.Container
	err := json.Unmarshal(containerJSON, &container)
	assert.Equal(t, err, nil)

	expectedDims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
		"collector":      "DockerStats",
	}
	expectedMetrics := []metric.Metric{
		metric.Metric{"DockerRxBytes", "gauge", 10, expectedDims},
		metric.Metric{"DockerTxBytes", "gauge", 20, expectedDims},
		metric.Metric{"DockerMemoryUsed", "gauge", 50, expectedDims},
		metric.Metric{"DockerMemoryLimit", "gauge", 70, expectedDims},
		metric.Metric{"DockerCpuPercentage", "gauge", 0.5, expectedDims},
	}

	d := getSUT()
	ret := d.buildMetrics(container, stats, 0.5)

	assert.Equal(t, ret, expectedMetrics)
}

func TestDockerStatsCalculateCPUPercent(t *testing.T) {
	var previousTotalUsage = uint64(0)
	var previousSystem = uint64(0)

	stats := new(docker.Stats)
	stats.CPUStats.CPUUsage.PercpuUsage = make([]uint64, 24)
	stats.CPUStats.CPUUsage.TotalUsage = 1261158030354
	stats.CPUStats.SystemCPUUsage = 108086414700000000

	assert.Equal(t, 0.02800332753427522, calculateCPUPercent(previousTotalUsage, previousSystem, stats))

	previousTotalUsage = stats.CPUStats.CPUUsage.TotalUsage
	previousSystem = stats.CPUStats.SystemCPUUsage
	stats.CPUStats.CPUUsage.TotalUsage = 1261164064229
	stats.CPUStats.SystemCPUUsage = 108086652820000000

	assert.Equal(t, 0.060815135225936505, calculateCPUPercent(previousTotalUsage, previousSystem, stats))
}

func TestGetInfoFromMesosTaskID(t *testing.T) {
	mesosID := "my--service.main.blablagit6bdsadnoise"
	serviceName, instanceName := getInfoFromMesosTaskID(mesosID)

	assert.Equal(t, serviceName, "my_service")
	assert.Equal(t, instanceName, "main")
}
