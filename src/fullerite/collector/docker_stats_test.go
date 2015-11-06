package collector

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/fsouza/go-dockerclient"
)

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
