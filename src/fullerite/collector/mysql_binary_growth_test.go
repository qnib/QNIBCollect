package collector

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"fullerite/metric"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func newMockMySQLBinlogGrowth() *MySQLBinlogGrowth {
	c := make(chan metric.Metric, 2)
	i := 10
	l := defaultLog
	return newMySQLBinlogGrowth(c, i, l)
}

func TestNewMySQLBinlogGrowth(t *testing.T) {
	c := make(chan metric.Metric)
	i := 10
	l := defaultLog.WithFields(l.Fields{"collector": "MySQLBinlog"})
	m := newMySQLBinlogGrowth(c, i, l)

	assert.Equal(t, m.Channel(), c)
	assert.Equal(t, m.Interval(), i)
	assert.Equal(t, m.log, l)
	assert.Equal(t, m.myCnfPath, defaultCnfPath)
}

func TestMySQLBinlogGrowthConfigure(t *testing.T) {
	m := newMockMySQLBinlogGrowth()
	config := map[string]interface{}{"mycnf": "my/cnf/path"}

	m.Configure(config)

	assert.Equal(t, m.myCnfPath, "my/cnf/path")
}

func TestMySQLBinlogGrowthConfigureEmpty(t *testing.T) {
	m := newMockMySQLBinlogGrowth()
	config := map[string]interface{}{}

	m.Configure(config)

	assert.Equal(t, m.myCnfPath, defaultCnfPath)
}

func mockGetBinlogGrowth(m *MySQLBinlogGrowth, binLog string, dataDir string, previousLog string, previousSize int64) (string, int64, int64) {
	return "log1", 1234, 30
}

func TestMySQLBinlogGrowthCollect(t *testing.T) {
	m := newMockMySQLBinlogGrowth()

	oldGetBinlogSize := getBinlogSize
	defer func() { getBinlogSize = oldGetBinlogSize }()
	getBinlogSize = func(m *MySQLBinlogGrowth, bilLog string, datadir string) (int64, error) { return int64(123456), nil }

	oldGetBinlogPath := getBinlogPath
	defer func() { getBinlogPath = oldGetBinlogPath }()
	getBinlogPath = func(m *MySQLBinlogGrowth) (string, string) { return "path/to/binlog", "/datadir" }

	m.Collect()

	select {
	case res := <-m.Channel():
		assert.Equal(t, res.Value, float64(123456))
	default:
		t.Fatal("The collect method did not emit anything")
	}
}

func TestMySQLBinlogGrowthCollectNoMyCnf(t *testing.T) {
	m := newMockMySQLBinlogGrowth()
	m.Configure(map[string]interface{}{"mycnf": "/non/existing/path"})

	m.Collect()

	select {
	case <-m.Channel():
		t.Fatal("The collect method shoudln't emit anything")
	default:
	}
}

func TestGetBinlogPathNoConfig(t *testing.T) {
	m := newMockMySQLBinlogGrowth()
	config := map[string]interface{}{"mycnf": "my/cnf/path"}
	m.Configure(config)

	binLog, dataDir := m.getBinlogPath()
	assert.Empty(t, binLog)
	assert.Empty(t, dataDir)
}

func TestGetBinlogPathNoSection(t *testing.T) {
	m := newMockMySQLBinlogGrowth()

	file, _ := ioutil.TempFile(os.TempDir(), "my.cnf")
	defer os.Remove(file.Name())

	config := map[string]interface{}{"mycnf": file.Name()}
	m.Configure(config)

	binLog, dataDir := m.getBinlogPath()
	assert.Equal(t, binLog, "")
	assert.Equal(t, dataDir, "")
}

func TestGetBinlogPath(t *testing.T) {
	m := newMockMySQLBinlogGrowth()

	file, _ := ioutil.TempFile("", "my.cnf")
	file.WriteString("[mysqld]\n")
	file.WriteString("log-bin = ../binlog\n")
	file.WriteString("datadir = /usr/local/data\n")
	defer os.Remove(file.Name())

	config := map[string]interface{}{"mycnf": file.Name()}
	m.Configure(config)

	binLog, dataDir := m.getBinlogPath()
	assert.Equal(t, "/usr/local/binlog", binLog)
	assert.Equal(t, "/usr/local/data", dataDir)
}

func TestGetBinlogPathAbsolute(t *testing.T) {
	m := newMockMySQLBinlogGrowth()

	file, _ := ioutil.TempFile("", "my.cnf")
	file.WriteString("[mysqld]\n")
	file.WriteString("log-bin = /usr/local/binlog\n")
	file.WriteString("datadir = /usr/local/data\n")
	defer os.Remove(file.Name())

	config := map[string]interface{}{"mycnf": file.Name()}
	m.Configure(config)

	binLog, dataDir := m.getBinlogPath()
	assert.Equal(t, "/usr/local/binlog", binLog)
	assert.Equal(t, "/usr/local/data", dataDir)
}

func TestgetBinlogSize(t *testing.T) {
	m := newMockMySQLBinlogGrowth()

	oldGetFileSize := getFileSize
	defer func() { getFileSize = oldGetFileSize }()
	getFileSize = func(filePath string) (int64, error) { return int64(123), nil }

	file, _ := ioutil.TempFile("", strings.Join([]string{"binlog", binLogFileSuffix}, "."))
	file.WriteString("../log1")
	file.WriteString("../log2")
	defer os.Remove(file.Name())

	size, err := m.getBinlogSize(file.Name(), "/datadir")
	assert.Equal(t, size, int64(246))
	assert.Nil(t, err)
}

func TestgetBinlogSizeNoIndex(t *testing.T) {
	m := newMockMySQLBinlogGrowth()

	_, err := m.getBinlogSize("/random/file", "/datadir")
	assert.NotNil(t, err)
}
