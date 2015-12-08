package collector

import (
	"bufio"
	"os"
	"path"
	"strings"

	"fullerite/metric"
	"fullerite/util"

	l "github.com/Sirupsen/logrus"
	"github.com/alyu/configparser"
)

const (
	defaultCnfPath   = "/etc/my.cnf"
	binLogFileSuffix = "index"
)

// MySQLBinlogGrowth collector
type MySQLBinlogGrowth struct {
	baseCollector
	myCnfPath string
}

// Dependency injection: Makes writing unit tests much easier, by being able to override these values in the *_test.go files.
var (
	getBinlogPath = (*MySQLBinlogGrowth).getBinlogPath
	getBinlogSize = (*MySQLBinlogGrowth).getBinlogSize
	getFileSize   = util.GetFileSize
)

// The MySQLBinlogGrowth collector emits the current size of all the binlog files as cumulative counter. This will show
// up in the graph as the rate of growth.
//
// The my.cnf config file contains the path to the binlog files (if it's not absolute, then it's relative to the
// datadir directory). The <binlog>.index file contains the list of the log files and their path.
//
// Example my.cnf format:
// [mysqld]
// log-bin = ../dir
// datadir = /var/srv/dir

// NewMySQLBinlogGrowth creates a new MySQLBinlogGrowth collector.
func NewMySQLBinlogGrowth(channel chan metric.Metric, initialInterval int, log *l.Entry) *MySQLBinlogGrowth {
	// Initialize the collector struct with the default values
	d := &MySQLBinlogGrowth{
		baseCollector: baseCollector{
			name:     "MySQLBinlogGrowth",
			log:      log,
			channel:  channel,
			interval: initialInterval,
		},
		myCnfPath: defaultCnfPath,
	}

	return d
}

// Configure takes a dictionary of values with which the handler can configure itself.
func (m *MySQLBinlogGrowth) Configure(configMap map[string]interface{}) {
	m.configureCommonParams(configMap)
	if myCnfPath, exists := configMap["mycnf"]; exists {
		m.myCnfPath = myCnfPath.(string)
	}
}

// Collect emits the tota size of the mysql binary logs
func (m *MySQLBinlogGrowth) Collect() {
	// read the bin-log and datadir values from my.cnf
	binLog, dataDir := getBinlogPath(m)
	if binLog == "" || dataDir == "" {
		return
	}

	size, err := getBinlogSize(m, strings.Join([]string{binLog, binLogFileSuffix}, "."), dataDir)

	if err == nil {
		metric := metric.Metric{
			Name:       "mysql.binlog_growth_rate",
			MetricType: metric.CumulativeCounter,
			Value:      float64(size),
			Dimensions: make(map[string]string),
		}

		m.Channel() <- metric
	}
}

// getBinlogPath read and parse the my.cnf config file and returns the path to the binlog file and datadir.
func (m *MySQLBinlogGrowth) getBinlogPath() (binLog string, dataDir string) {
	// read my.cnf config file
	config, err := configparser.Read(m.myCnfPath)
	if err != nil {
		m.log.Error(err)
		return
	}

	section, err := config.Section("mysqld")
	if err != nil {
		m.log.Error("mysqld section missing in ", m.myCnfPath)
		return
	}

	binLog = section.ValueOf("log-bin")
	if binLog == "" {
		m.log.Error("log-bin value missing from ", m.myCnfPath)
		return
	}

	dataDir = section.ValueOf("datadir")
	if dataDir == "" {
		m.log.Error("datadir value missing from ", m.myCnfPath)
		return
	}

	// If the log-bin value is a relative path then it's based on datadir
	if !path.IsAbs(binLog) {
		binLog = path.Join(dataDir, binLog)
	}
	return
}

// getBinlogSize returns the total size of the binlog files
func (m *MySQLBinlogGrowth) getBinlogSize(binLog string, dataDir string) (size int64, err error) {
	// Read the binlog.index file
	// It contains a list of log files, one per line
	file, err := os.Open(binLog)
	if err != nil {
		m.log.Warn("Cannot open index file ", binLog)
		return
	}
	defer file.Close()

	// Read the file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fileName := scanner.Text()
		// If the fileName is not an absolute path, then it's relative to the datadir directory
		if !path.IsAbs(fileName) {
			fileName = path.Join(dataDir, fileName)
		}

		file_size, err := getFileSize(fileName)
		if err != nil {
			m.log.Warn(err)
		}
		size += file_size
	}

	if scanErr := scanner.Err(); scanErr != nil {
		m.log.Warn("There was an error reading the index file")
		return
	}
	return
}
