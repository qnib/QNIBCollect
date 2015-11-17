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
	expected_chan := make(chan metric.Metric)
	var expected_logger = defaultLog.WithFields(l.Fields{"collector": "fullerite"})

	return NewDockerStats(expected_chan, 10, expected_logger)
}

func TestDockerStatsNewDockerStats(t *testing.T) {
	expected_chan := make(chan metric.Metric)
	var expected_logger = defaultLog.WithFields(l.Fields{"collector": "fullerite"})
	expected_type := make(map[string]*CPUValues)

	d := NewDockerStats(expected_chan, 10, expected_logger)

	assert.Equal(t, d.log, expected_logger)
	assert.Equal(t, d.channel, expected_chan)
	assert.Equal(t, d.interval, 10)
	assert.Equal(t, d.name, "DockerStats")
	assert.Equal(t, reflect.TypeOf(d.previousCPUValues), reflect.TypeOf(expected_type))
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

	containerJson := []byte(`
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
	err := json.Unmarshal(containerJson, &container)
	assert.Equal(t, err, nil)

	expected_dims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
		"instance_name":  "main",
		"collector":      "DockerStats",
	}
	expected_metrics := []metric.Metric{
		metric.Metric{"DockerRxBytes", "gauge", 10, expected_dims},
		metric.Metric{"DockerTxBytes", "gauge", 20, expected_dims},
		metric.Metric{"DockerMemoryUsed", "gauge", 50, expected_dims},
		metric.Metric{"DockerMemoryLimit", "gauge", 70, expected_dims},
		metric.Metric{"DockerCpuPercentage", "gauge", 0.5, expected_dims},
	}

	d := getSUT()
	ret := d.buildMetrics(container, stats, 0.5)

	assert.Equal(t, ret, expected_metrics)
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
