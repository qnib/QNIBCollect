package collector

import (
	"fullerite/config"
	"fullerite/metric"

	"strings"
	"time"

	l "github.com/Sirupsen/logrus"

	"github.com/fsouza/go-dockerclient"
)

const (
	mesosTaskID      = "MESOS_TASK_ID"
	endpoint         = "unix:///var/run/docker.sock"
	serviceNameLabel = "SERVICE_NAME"
)

// DockerStats collector type.
// previousCPUValues contains the last cpu-usage values per container.
// dockerClient is the client for the Docker remote API.
type DockerStats struct {
	baseCollector
	previousCPUValues map[string]*CPUValues
	dockerClient      *docker.Client
	statsTimeout      int
}

// CPUValues struct contains the last cpu-usage values in order to compute properly the current values.
// (see calculateCPUPercent() for more details)
type CPUValues struct {
	totCPU, systemCPU uint64
}

// NewDockerStats creates a new DockerStats collector.
func NewDockerStats(channel chan metric.Metric, initialInterval int, log *l.Entry) *DockerStats {
	d := new(DockerStats)

	d.log = log
	d.channel = channel
	d.interval = initialInterval

	d.name = "DockerStats"
	d.previousCPUValues = make(map[string]*CPUValues)
	d.dockerClient, _ = docker.NewClient(endpoint)

	return d
}

// Configure takes a dictionary of values with which the handler can configure itself.
func (d *DockerStats) Configure(configMap map[string]interface{}) {
	d.configureCommonParams(configMap)
	if timeout, exists := configMap["dockerStatsTimeout"]; exists {
		d.statsTimeout = min(config.GetAsInt(timeout, d.interval), d.interval)
	} else {
		d.statsTimeout = d.interval
	}
}

// Collect iterates on all the docker containers alive and, if possible, collects the correspondent
// memory and cpu statistics.
// For each container a gorutine is started to spin up the collection process.
func (d *DockerStats) Collect() {
	if d.dockerClient == nil {
		d.log.Error("Invalid endpoint: ", docker.ErrInvalidEndpoint)
		return
	}
	containers, err := d.dockerClient.ListContainers(docker.ListContainersOptions{All: false})
	if err != nil {
		d.log.Error("ListContainers() failed: ", err)
		return
	}
	for _, apiContainer := range containers {
		container, err := d.dockerClient.InspectContainer(apiContainer.ID)
		if err != nil {
			d.log.Error("InspectContainer() failed: ", err)
			continue
		}
		if _, ok := d.previousCPUValues[container.ID]; !ok {
			d.previousCPUValues[container.ID] = new(CPUValues)
		}
		go d.getDockerContainerInfo(container)
	}
}

// getDockerContainerInfo gets container statistics for the given container.
// results is a channel to make possible the synchronization between the main process and the gorutines (wait-notify pattern).
func (d DockerStats) getDockerContainerInfo(container *docker.Container) {
	errC := make(chan error, 1)
	statsC := make(chan *docker.Stats, 1)
	done := make(chan bool)

	go func() {
		errC <- d.dockerClient.Stats(docker.StatsOptions{container.ID, statsC, false, done, time.Second * time.Duration(d.interval)})
	}()
	select {
	case stats, ok := <-statsC:
		if !ok {
			err := <-errC
			d.log.Error("Failed to collect docker container stats: ", err)
			break
		}
		done <- true

		ret := d.buildMetrics(container, stats, calculateCPUPercent(d.previousCPUValues[container.ID].totCPU, d.previousCPUValues[container.ID].systemCPU, stats))

		d.sendMetrics(ret)

		d.previousCPUValues[container.ID].totCPU = stats.CPUStats.CPUUsage.TotalUsage
		d.previousCPUValues[container.ID].systemCPU = stats.CPUStats.SystemCPUUsage

		break
	case <-time.After(time.Duration(d.statsTimeout) * time.Second):
		d.log.Error("Timed out collecting stats for container ", container.ID)
		done <- true
		break
	}
}

// buildMetrics creates the actual metrics for the given container.
func (d DockerStats) buildMetrics(container *docker.Container, containerStats *docker.Stats, cpuPercentage float64) []metric.Metric {
	ret := []metric.Metric{
		buildDockerMetric("DockerRxBytes", float64(containerStats.Network.RxBytes)),
		buildDockerMetric("DockerTxBytes", float64(containerStats.Network.TxBytes)),
		buildDockerMetric("DockerMemoryUsed", float64(containerStats.MemoryStats.Usage)),
		buildDockerMetric("DockerMemoryLimit", float64(containerStats.MemoryStats.Limit)),
		buildDockerMetric("DockerCpuPercentage", cpuPercentage),
	}
	additionalDimensions := map[string]string{
		"container_id":   container.ID,
		"container_name": strings.TrimPrefix(container.Name, "/"),
	}
	metric.AddToAll(&ret, additionalDimensions)
	metric.AddToAll(&ret, getServiceDimensions(container))

	return ret
}

// sendMetrics writes all the metrics received to the collector channel.
func (d DockerStats) sendMetrics(metrics []metric.Metric) {
	for _, m := range metrics {
		d.Channel() <- m
	}
}

// Function that extracts the service and instance name from mesos id in order to add them as dimensions
// in these metrics.
func getServiceDimensions(container *docker.Container) map[string]string {
	envVars := container.Config.Env

	tmp := make(map[string]string)
	for _, envVariable := range envVars {
		envArray := strings.Split(envVariable, "=")
		if envArray[0] == mesosTaskID {
			serviceName, instance := getInfoFromMesosTaskID(envArray[1])
			tmp["service_name"] = serviceName
			tmp["instance_name"] = instance
			break
		} else if envArray[0] == serviceNameLabel {
			tmp["service_name"] = envArray[1]
		}
	}
	return tmp
}

func getInfoFromMesosTaskID(taskID string) (serviceName, instance string) {
	varArray := strings.Split(taskID, ".")
	return strings.Replace(varArray[0], "--", "_", -1), strings.Replace(varArray[1], "--", "_", -1)
}

func buildDockerMetric(name string, value float64) (m metric.Metric) {
	m = metric.New(name)
	m.Value = value
	m.AddDimension("collector", "DockerStats")
	return m
}

// Function that compute the current cpu usage percentage combining current and last values.
func calculateCPUPercent(previousCPU, previousSystem uint64, stats *docker.Stats) float64 {
	var (
		cpuPercent = 0.0
		// calculate the change for the cpu usage of the container in between readings
		cpuDelta = float64(stats.CPUStats.CPUUsage.TotalUsage - previousCPU)
		// calculate the change for the entire system between readings
		systemDelta = float64(stats.CPUStats.SystemCPUUsage - previousSystem)
	)

	if systemDelta > 0.0 && cpuDelta > 0.0 {
		cpuPercent = (cpuDelta / systemDelta) * float64(len(stats.CPUStats.CPUUsage.PercpuUsage)) * 100.0
	}
	return cpuPercent
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
