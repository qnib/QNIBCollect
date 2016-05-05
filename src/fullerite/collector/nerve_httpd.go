package collector

import (
	"bufio"
	"bytes"
	"fmt"
	"fullerite/metric"
	"fullerite/util"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	l "github.com/Sirupsen/logrus"
)

var (
	getNerveHTTPDMetrics = (*NerveHTTPD).getMetrics
	knownApacheMetrics   = []string{
		"ReqPerSec", "BytesPerSec", "BytesPerReq", "BusyWorkers",
		"Total Accesses", "IdleWorkers", "StartingWorkers",
		"ReadingWorkers", "WritingWorkers", "KeepaliveWorkers",
		"DnsWorkers", "ClosingWorkers", "LoggingWorkers",
		"FinishingWorkers", "CleanupWorkers", "StandbyWorkers", "CPULoad",
	}
	metricRegexp = regexp.MustCompile(`^([A-Za-z ]+):\s+(.+)$`)
)

// NerveHTTPD discovers Apache servers via Nerve config
// and reports metric for them
type NerveHTTPD struct {
	baseCollector

	configFilePath  string
	queryPath       string
	timeout         int
	statusTTL       time.Duration
	failedEndPoints map[string]int64
	mu              *sync.RWMutex
}

type nerveHTTPDResponse struct {
	data   []byte
	err    error
	status int
}

func init() {
	RegisterCollector("NerveHTTPD", newNerveHTTPD)
}

func newNerveHTTPD(channel chan metric.Metric, initialInterval int, log *l.Entry) Collector {
	c := new(NerveHTTPD)
	c.channel = channel
	c.interval = initialInterval
	c.log = log
	c.mu = new(sync.RWMutex)

	c.name = collectorName
	c.configFilePath = "/etc/nerve/nerve.conf.json"
	c.queryPath = "server-status?auto"
	c.timeout = 2
	c.statusTTL = time.Duration(60) * time.Minute
	c.failedEndPoints = map[string]int64{}
	return c
}

// Configure the collector
func (c *NerveHTTPD) Configure(configMap map[string]interface{}) {
	if val, exists := configMap["queryPath"]; exists {
		c.queryPath = val.(string)
	}
	if val, exists := configMap["configFilePath"]; exists {
		c.configFilePath = val.(string)
	}

	if val, exists := configMap["status_ttl"]; exists {
		if t, ok := val.(int); ok {
			c.statusTTL = time.Duration(t) * time.Second
		}
	}

	c.configureCommonParams(configMap)
}

// Collect the metrics
func (c *NerveHTTPD) Collect() {
	rawFileContents, err := ioutil.ReadFile(c.configFilePath)
	if err != nil {
		c.log.Warn("Failed to read the contents of file ", c.configFilePath, " because ", err)
		return
	}

	servicePortMap, err := util.ParseNerveConfig(&rawFileContents)
	if err != nil {
		c.log.Warn("Failed to parse the nerve config at ", c.configFilePath, ": ", err)
		return
	}
	c.log.Debug("Finished parsing Nerve config into ", servicePortMap)

	for port, serviceName := range servicePortMap {
		if !c.checkIfFailed(serviceName, port) {
			go c.emitHTTPDMetric(serviceName, port)
		}
	}
}

func (c *NerveHTTPD) emitHTTPDMetric(serviceName string, port int) {
	metrics := getNerveHTTPDMetrics(c, serviceName, port)
	for _, metric := range metrics {
		c.Channel() <- metric
	}
}

func (c *NerveHTTPD) checkIfFailed(serviceName string, port int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	endpoint := fmt.Sprintf("%s:%d", serviceName, port)
	if lastFailed, ok := c.failedEndPoints[endpoint]; ok {
		tm := time.Unix(lastFailed, 0)
		if time.Since(tm) < c.statusTTL {
			return true
		}
	}
	return false
}

func (c *NerveHTTPD) getMetrics(serviceName string, port int) []metric.Metric {
	results := []metric.Metric{}
	serviceLog := c.log.WithField("service", serviceName)

	endpoint := fmt.Sprintf("http://localhost:%d/%s", port, c.queryPath)
	serviceLog.Debug("making GET request to ", endpoint)

	httpResponse := fetchApacheMetrics(endpoint, port)

	if httpResponse.status != 200 {
		c.updateFailedStatus(serviceName, port, httpResponse.status)
		serviceLog.Warn("Failed to query endpoint ", endpoint, ": ", httpResponse.err)
		return results
	}
	return extractApacheMetrics(httpResponse.data)
}

func extractApacheMetrics(data []byte) []metric.Metric {
	results := []metric.Metric{}
	reader := bytes.NewReader(data)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		metricLine := scanner.Text()
		resultMatch := metricRegexp.FindStringSubmatch(metricLine, -1)
		k := resultMatch[0]
		v := resultMatch[1]
		if k == "IdleWorkers" {
			continue
		}

		if k == "Scoreboard" {
			scoreBoardMetrics := extractScoreBoardMetrics(k, v)
			results = append(results, scoreBoardMetrics)
		}

		results = append(results, buildApacheMetric(k, v))
	}
	return results
}

func buildApacheMetric(key, value string) metric.Metric {

}

func extractScoreBoardMetrics(key, value string) []metric.Metric {
	results := []metric.Metric{}
	results = append(results, metric.WithValue("IdleWorkers", strings.Count(value, "_")))
	results = append(results, metric.WithValue("StartingWorkers", strings.Count(value, "S")))
	results = append(results, metric.WithValue("ReadingWorkers", strings.Count(value, "R")))
	results = append(results, metric.WithValue("WritingWorkers", strings.Count(value, "W")))
	results = append(results, metric.WithValue("KeepaliveWorkers", strings.Count(value, "K")))
	results = append(results, metric.WithValue("DnsWorkers", strings.Count(value, "D")))
	results = append(results, metric.WithValue("ClosingWorkers", strings.Count(value, "C")))
	results = append(results, metric.WithValue("LoggingWorkers", strings.Count(value, "L")))
	results = append(results, metric.WithValue("FinishingWorkers", strings.Count(value, "G")))
	results = append(results, metric.WithValue("CleanupWorkers", strings.Count(value, "I")))
	results = append(results, metric.WithValue("StandbyWorkers", strings.Count(value, "_")))
	return results
}

func (c *NerveHTTPD) updateFailedStatus(serviceName string, port int, statusCode int) {
	if statusCode == 404 {
		c.mu.Lock()
		defer c.mu.Unlock()
		endpoint := fmt.Sprintf("%s:%d", serviceName, port)
		c.failedEndPoints[endpoint] = time.Now().Unix()
	}

}

func fetchApacheMetrics(endpoint string, timeout int) *nerveHTTPDResponse {
	response := new(nerveHTTPDResponse)
	client := http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}

	rsp, err := client.Get(endpoint)
	response.err = err
	response.status = rsp.StatusCode
	if err != nil {
		return response
	}

	txt, err := ioutil.ReadAll(rsp.Body)
	defer rsp.Body.Close()
	if err != nil {
		response.err = err
		return response
	}
	response.data = txt
	return response
}
