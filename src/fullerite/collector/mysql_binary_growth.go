package collector

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path"

	"fullerite/metric"

	l "github.com/Sirupsen/logrus"
	"github.com/alyu/configparser"
)

const (
	defaultCnfPath   = "/etc/my.cnf"
	binLogFileSuffix = ".index"
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
	getFileSize   = (*MySQLBinlogGrowth).getFileSize
)

// The MySQLBinlogGrowth collector emits the current size of all the binlog files as cumulative counter. This will show
// up in SignalFx as the rate of growth.
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

// Collect computes the difference between the current bin-log size and the one at the previous run and
// emits the rate of change per second
func (m *MySQLBinlogGrowth) Collect() {
	// read the bin-log and datadir values from my.cnf
	binLog, dataDir := getBinlogPath(m)
	if binLog == "" || dataDir == "" {
		return
	}

	size, err := getBinlogSize(m, binLog+binLogFileSuffix, dataDir)

	if err == nil {
		metric := metric.Metric{
			Name:       "mysql.binlog_growth_rate",
			MetricType: metric.CumulativeCounter,
			Value:      float64(size),
			Dimensions: map[string]string{},
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
		return "", ""
	}

	section, err := config.Section("mysqld")
	if err != nil {
		m.log.Error("mysqld section missing in ", m.myCnfPath)
		return "", ""
	}

	binLog = section.ValueOf("log-bin")
	dataDir = section.ValueOf("datadir")
	// If the log-bin value is a relative path then it's based on datadir
	if !path.IsAbs(binLog) {
		binLog = path.Join(dataDir, binLog)
	}
	return
}

// getFileSize returns the size in bytes of the specified file
func (m *MySQLBinlogGrowth) getFileSize(filePath string) int64 {
	file, err := os.Open(filePath)
	if err != nil {
		return 0
	}
	fi, err := file.Stat()
	if err != nil {
		return 0
	}
	return fi.Size()
}

// getBinlogSize returns the total size of the binlog files
func (m *MySQLBinlogGrowth) getBinlogSize(binLog string, dataDir string) (size int64, err error) {
	size = 0
	err = nil

	// Read the binlog.index file
	// It contains a list of log files, one per line
	file, err := os.Open(binLog)
	if err != nil {
		m.log.Warn("Cannot open index file ", binLog)
		return 0, fmt.Errorf("Cannot open index file %s", binLog)
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

		size += getFileSize(m, fileName)
	}

	if err := scanner.Err(); err != nil {
		m.log.Warn("There was an error reading the index file")
		return 0, errors.New("There was an error reading the index file")
	}
	return
}
