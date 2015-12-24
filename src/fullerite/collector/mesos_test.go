package collector

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fullerite/metric"
	"fullerite/util"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// MockMLE Mock for MesosLeaderElectInterface that encapsulate functionality to check if the interface methods have been called/not. We embed the interface for the case of the test because this interface has unexported methods (set()). And since the interface comes from a different package, we cannot provide a concrete implementation for it.
type MockMLE struct {
	util.MesosLeaderElectInterface
	ConfigureCalled bool
}

func (m *MockMLE) Configure(nodes string, ttl time.Duration) {
	m.ConfigureCalled = true
}

func (m *MockMLE) Get() string {
	return httptest.DefaultRemoteAddr
}

// mockExternalIP Injectable mock for externalIP, for test assertions.
func mockExternalIP() (string, error) {
	return httptest.DefaultRemoteAddr, nil
}

func TestMesosStatsNewMesosStats(t *testing.T) {
	oldExternalIP := externalIP
	defer func() { externalIP = oldExternalIP }()

	externalIP = mockExternalIP

	c := make(chan metric.Metric)
	i := 10
	l := defaultLog.WithFields(l.Fields{"collector": "Mesos"})

	sut := NewMesosStats(c, i, l)

	assert.Equal(t, c, sut.channel)
	assert.Equal(t, i, sut.interval)
	assert.Equal(t, l, sut.log)
	assert.Equal(t, httptest.DefaultRemoteAddr, sut.IP)
	assert.Equal(t, http.Client{Timeout: getTimeout}, sut.client)
}

func TestMesosStatsConfigure(t *testing.T) {
	oldNewMLE := newMLE
	defer func() { newMLE = oldNewMLE }()

	newMLE = func() util.MesosLeaderElectInterface { return &MockMLE{} }

	tests := []struct {
		config map[string]interface{}
		isNil  bool
		msg    string
	}{
		{map[string]interface{}{}, true, "Config does not contain mesosNodes, so Configure should fail."},
		{map[string]interface{}{"mesosNodes": ""}, true, "Config contains empty mesosNodes, so Configure should fail."},
		{map[string]interface{}{"mesosNodes": "ip1,ip2"}, false, "Config contains mesosNodes, so Configure should work."},
	}

	for _, test := range tests {
		config := test.config
		sut := NewMesosStats(nil, 0, defaultLog)

		assert.Nil(t, sut.mesosCache, "Before *baseCollector.Configure() is called, MesosStats.mesosCache should not be created.")

		sut.Configure(config)

		switch test.isNil {
		case true:
			assert.Nil(t, sut.mesosCache, test.msg)
		case false:
			assert.NotNil(t, sut.mesosCache, test.msg)
			mock, _ := sut.mesosCache.(*MockMLE)
			assert.True(t, mock.ConfigureCalled, "*MesosLeaderElect.Configure() should be called.")
		}

	}
}

func TestMesosStatsCollect(t *testing.T) {
	oldExternalIP := externalIP
	oldNewMLE := newMLE
	oldSendMetrics := sendMetrics
	defer func() {
		externalIP = oldExternalIP
		newMLE = oldNewMLE
		sendMetrics = oldSendMetrics
	}()

	newMLE = func() util.MesosLeaderElectInterface { return &MockMLE{} }

	sendMetricsCalled := false
	c := make(chan bool)
	sendMetrics = func(m *MesosStats) {
		sendMetricsCalled = true
		c <- true
	}

	tests := []struct {
		configMap           map[string]interface{}
		externalIP          string
		isSendMetricsCalled bool
		msg                 string
	}{
		{map[string]interface{}{"mesosNodes": ""}, httptest.DefaultRemoteAddr, false, "Invalid collector config, therefore no mesosCache is initialised."},
		{map[string]interface{}{"mesosNodes": "ip1,ip2"}, "5.6.7.8", false, "Machine IP is not equal to leader IP, therefore we should skip collection."},
		{map[string]interface{}{"mesosNodes": "ip1,ip2"}, httptest.DefaultRemoteAddr, true, "Current box is leader; therefore, we should be called sendMetrics."},
	}

	for _, test := range tests {
		sendMetricsCalled = false
		configMap := test.configMap
		externalIP = func() (string, error) { return test.externalIP, nil }

		sut := NewMesosStats(nil, 0, defaultLog)
		sut.Configure(configMap)
		sut.Collect()

		switch test.isSendMetricsCalled {
		case false:
			assert.False(t, sendMetricsCalled, test.msg)
		case true:
			<-c
			assert.True(t, sendMetricsCalled, test.msg)
		}
	}
}

func TestMesosStatsSendMetrics(t *testing.T) {
	oldGetMetrics := getMetrics
	defer func() { getMetrics = oldGetMetrics }()

	expected := metric.Metric{"test", "gauge", 0.1, map[string]string{}}
	getMetrics = func(m *MesosStats, ip string) map[string]float64 {
		return map[string]float64{
			"test": 0.1,
		}
	}

	c := make(chan metric.Metric)
	sut := NewMesosStats(c, 10, defaultLog)

	go sut.sendMetrics()
	actual := <-c

	assert.Equal(t, expected, actual)
}

func TestMesosStatsGetMetrics(t *testing.T) {
	oldGetMetricsURL := getMetricsURL
	defer func() {
		getMetricsURL = oldGetMetricsURL
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

		getMetricsURL = func(ip string) string { return ts.URL }

		sut := NewMesosStats(nil, 10, defaultLog)
		actual := getMetrics(sut, httptest.DefaultRemoteAddr)

		assert.Equal(t, expected, actual)
	}
}

func TestMesosStatsGetMetricsHandleErrors(t *testing.T) {
	oldGetMetricsURL := getMetricsURL
	defer func() {
		getMetricsURL = oldGetMetricsURL
	}()

	getMetricsURL = func(ip string) string { return "" }

	sut := NewMesosStats(nil, 10, defaultLog)
	actual := getMetrics(sut, httptest.DefaultRemoteAddr)

	assert.Nil(t, actual, "Empty (invalid) URL, which means http client should throw an error; therefore, we expect a nil from getMetrics")
}

func TestMesosStatsGetMetricsHandleNon200s(t *testing.T) {
	oldGetMetricsURL := getMetricsURL
	defer func() {
		getMetricsURL = oldGetMetricsURL
	}()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		fmt.Fprintln(w, `Custom error`)
	}))
	defer ts.Close()

	getMetricsURL = func(ip string) string { return ts.URL }

	sut := NewMesosStats(nil, 10, defaultLog)
	actual := getMetrics(sut, httptest.DefaultRemoteAddr)

	assert.Nil(t, actual, "Server threw a 500, so we should expect nil from getMetrics")
}

func TestMesosStatsBuildMetric(t *testing.T) {
	expected := metric.Metric{"test", "gauge", 0.1, map[string]string{}}

	actual := buildMetric("test", 0.1)

	assert.Equal(t, expected, actual)
}

func TestMesosStatsBuildMetricCumCounter(t *testing.T) {
	expected := metric.Metric{"master.slave_reregistrations", metric.CumulativeCounter, 0.1, map[string]string{}}

	actual := buildMetric("master.slave_reregistrations", 0.1)

	assert.Equal(t, expected, actual)
}
