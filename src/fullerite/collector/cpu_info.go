package collector

import (
	"fullerite/metric"

	"bufio"
	"os"
	"strings"

	l "github.com/Sirupsen/logrus"
)

const (
	collectorName   = "CPUInfo"
	metricName      = "cpu_info"
	defaultProcPath = "/proc/cpuinfo"
)

var knownManufacturers = [...]string{"AMD", "Processor", "Intel(R)", "CPU"}

// CPUInfo collector type
// Collect the CPU count and model name
type CPUInfo struct {
	baseCollector
	metricName string
	procPath   string
}

func init() {
	RegisterCollector("CPUInfo", newCPUInfo)
}

// newCPUInfo Simple constructor for CPUInfo collector
func newCPUInfo(channel chan metric.Metric, initialInterval int, log *l.Entry) Collector {
	c := new(CPUInfo)
	c.channel = channel
	c.interval = initialInterval
	c.log = log

	c.name = collectorName
	c.metricName = metricName
	c.procPath = defaultProcPath
	return c
}

// Configure Override default parameters
func (c *CPUInfo) Configure(configMap map[string]interface{}) {
	if procPath, exists := configMap["procPath"]; exists == true {
		c.procPath = procPath.(string)
	}
	c.configureCommonParams(configMap)
}

// Collect Emits the no of CPUs and ModelName
func (c *CPUInfo) Collect() {
	value, model, err := c.getCPUInfo()
	if err != nil {
		c.log.Error("Error while collecting metrics: ", err)
		return
	}
	metric := metric.New(c.metricName)
	metric.Value = value
	metric.AddDimension("model", model)
	c.Channel() <- metric
	c.metricCounter = 1
	c.log.Debug(metric)
}

func (c CPUInfo) getCPUInfo() (float64, string, error) {

	// Prepare to read file
	file, err := os.Open(c.procPath)
	if err != nil {
		c.log.Error("Unable to read file: ", err)
		return 0.0, "", err
	}
	defer file.Close()

	// Read file contents and gather metrics
	physIds := map[string]bool{}
	modelName := ""
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "physical id") {
			physIds[getValueFromLine(line)] = true
		} else if strings.HasPrefix(line, "model name") {
			val := getValueFromLine(line)
			if modelName == "" {
				modelName = val
			} else if modelName != val {
				modelName = "mixed"
			}
		}
	}

	err = scanner.Err()
	if err != nil {
		c.log.Error("Error while trying to scan through file: ", err)
	}
	modelName = removeCommonManufacturersName(modelName)
	return float64(len(physIds)), modelName, err
}

func getValueFromLine(line string) string {
	elems := strings.Split(line, ":")
	return strings.TrimSpace(elems[1])
}

func removeCommonManufacturersName(line string) string {
	elems := strings.Fields(line)
	if len(elems) != 0 {
		for _, manufacturer := range knownManufacturers {
			if elems[0] == manufacturer {
				return strings.Join(elems[1:], " ")
			}
		}
	}
	return line
}
