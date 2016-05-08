package collector

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fullerite/metric"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// mockGetSlaveExternalIP Injectable mock for externalIP, for test assertions.
func mockGetSlaveExternalIP() (string, error) {
	return httptest.DefaultRemoteAddr, nil
}

func TestNewMesosSlaveStats(t *testing.T) {
	oldExternalIP := getSlaveExternalIP
	defer func() { getSlaveExternalIP = oldExternalIP }()

	getSlaveExternalIP = mockGetSlaveExternalIP

	c := make(chan metric.Metric)
	i := 10
	l := defaultLog.WithFields(l.Fields{"collector": "Mesos"})

	sut := newMesosSlaveStats(c, i, l).(*MesosSlaveStats)

	assert.Equal(t, c, sut.channel)
	assert.Equal(t, i, sut.interval)
	assert.Equal(t, l, sut.log)
	assert.Equal(t, httptest.DefaultRemoteAddr, sut.IP)
	assert.Equal(t, mesosDefaultSlaveSnapshotPort, sut.snapshotPort)
	assert.Equal(t, time.Duration(httpDefaultTimeout)*time.Second, sut.client.Timeout)
}

func TestMesosSlaveStatsConfigureDefault(t *testing.T) {
	config := make(map[string]interface{})
	c := make(chan metric.Metric)
	sut := newMesosSlaveStats(c, 10, defaultLog).(*MesosSlaveStats)

	sut.Configure(config)

	assert.Equal(t, mesosDefaultSlaveSnapshotPort, sut.snapshotPort)
	assert.Equal(t, time.Duration(httpDefaultTimeout)*time.Second, sut.client.Timeout)
	assert.Equal(t, time.Duration(httpDefaultTimeout)*time.Second, sut.client.Timeout)
}

func TestMesosSlaveStatsConfigure(t *testing.T) {
	config := map[string]interface{}{
		"httpTimeout":       "15",
		"slaveSnapshotPort": "1234",
	}
	c := make(chan metric.Metric)
	sut := newMesosSlaveStats(c, 10, defaultLog).(*MesosSlaveStats)

	sut.Configure(config)

	assert.Equal(t, 1234, sut.snapshotPort)
	assert.Equal(t, time.Duration(15)*time.Second, sut.client.Timeout)
	assert.Equal(t, time.Duration(15)*time.Second, sut.client.Timeout)
}

func TestMesosSlaveStatsSendMetrics(t *testing.T) {
	oldGetMetrics := getSlaveMetrics
	defer func() { getSlaveMetrics = oldGetMetrics }()

	now := time.Now()
	expected := metric.Metric{"mesos.test", "gauge", 0.1, map[string]string{}, now}
	getSlaveMetrics = func(m *MesosSlaveStats, ip string) map[string]float64 {
		return map[string]float64{
			"test": 0.1,
		}
	}

	c := make(chan metric.Metric)
	sut := newMesosSlaveStats(c, 10, defaultLog).(*MesosSlaveStats)

	go sut.sendMetrics()
	actual := <-c
	actual.SetTime(now)
	assert.Equal(t, expected, actual)
}

func TestMesosSlaveStatsGetMetrics(t *testing.T) {
	oldGetMetricsURL := getSlaveMetricsURL
	defer func() {
		getSlaveMetricsURL = oldGetMetricsURL
	}()

	tests := []struct {
		rawResponse string
		expected    map[string]float64
		msg         string
	}{
		{"{\"frameworks\\/chronos\\/messages_processed\":6784068}", map[string]float64{"frameworks.chronos.messages_processed": 6784068}, "Valid JSON should return valid metrics."},
		{"{\"frameworks\\/chronos\\/messages_processed6784068}", nil, "Invalid JSON should return nil."},
	}

	for _, test := range tests {
		expected := test.expected
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, test.rawResponse)
		}))
		defer ts.Close()

		getSlaveMetricsURL = func(m *MesosSlaveStats, ip string) string { return ts.URL }

		sut := newMesosSlaveStats(nil, 10, defaultLog).(*MesosSlaveStats)
		actual := sut.getSlaveMetrics(httptest.DefaultRemoteAddr)

		assert.Equal(t, expected, actual)
	}
}

func TestMesosSlaveStatsGetMetricsHandleErrors(t *testing.T) {
	oldGetMetricsURL := getSlaveMetricsURL
	defer func() {
		getSlaveMetricsURL = oldGetMetricsURL
	}()

	getSlaveMetricsURL = func(m *MesosSlaveStats, ip string) string { return "" }

	sut := newMesosSlaveStats(nil, 10, defaultLog).(*MesosSlaveStats)
	actual := sut.getSlaveMetrics(httptest.DefaultRemoteAddr)

	assert.Nil(t, actual, "Empty (invalid) URL, which means http client should throw an error; therefore, we expect a nil from getMetrics")
}

func TestMesosSlaveStatsGetMetricsHandleNon200s(t *testing.T) {
	oldGetMetricsURL := getSlaveMetricsURL
	defer func() {
		getSlaveMetricsURL = oldGetMetricsURL
	}()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		fmt.Fprintln(w, `Custom error`)
	}))
	defer ts.Close()

	getSlaveMetricsURL = func(m *MesosSlaveStats, ip string) string { return ts.URL }

	sut := newMesosSlaveStats(nil, 10, defaultLog).(*MesosSlaveStats)
	actual := sut.getSlaveMetrics(httptest.DefaultRemoteAddr)

	assert.Nil(t, actual, "Server threw a 500, so we should expect nil from getMetrics")
}

func TestMesosSlaveStatsBuildMetric(t *testing.T) {
	sut := newMesosSlaveStats(nil, 10, defaultLog).(*MesosSlaveStats)

	tests := []struct {
		name       string
		MetricType string
	}{
		{"slave.mem_used", metric.Gauge},
		{"slave.tasks_killed", metric.CumulativeCounter},
	}

	for _, test := range tests {
		metric := sut.buildMetric(test.name, 1)
		assert.Equal(t, test.MetricType, metric.MetricType)
	}
}
