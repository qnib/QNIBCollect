package collector

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOpenTSDBConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})

	c := newOpenTSDB(nil, 12, nil).(*OpenTSDB)
	c.Configure(config)

	assert.Equal(t,
		c.Interval(),
		12,
		"should be the default collection interval",
	)
}

func TestOpenTSDBConfigure(t *testing.T) {
	config := make(map[string]interface{})
	config["interval"] = 9999
	config["port"] = "4242"
	c := newOpenTSDB(nil, 12, nil).(*OpenTSDB)
	c.Configure(config)

	assert := assert.New(t)
	assert.Equal(c.Interval(), 9999, "should be the defined interval")
	assert.Equal(c.Port(), "4242", "should be the defined port")
}

/*
func TestOpenTSBDCollect(t *testing.T) {
	config := make(map[string]interface{})
	config["port"] = "0"

	testChannel := make(chan metric.Metric)
	testLog := test_utils.BuildLogger()

	c := newOpenTSDB(testChannel, 123, testLog).(*OpenTSDB)
	c.Configure(config)

	// start collecting metrics
	go c.Collect()

	conn, err := connectToOpenTSDBCollector(d)
	require.Nil(t, err, "should connect")
	require.NotNil(t, conn, "should connect")

	fmt.Fprintf(conn, string(b)+"\n")

	select {
	case m := <-c.Channel():
		assert.Equal(t, m.Name, "test")
	case <-time.After(1 * time.Second):
		t.Fail()
	}
}
*/

func TestParseOpenTSDBMetric(t *testing.T) {
	rawData := "put sys.cpu.user host=webserver01,cpu=0 1356998400 1"
	c := newOpenTSDB(nil, 12, nil).(*OpenTSDB)
	var conf map[string]interface{}
	c.Configure(conf)
	metric, ok := c.parseMetric(rawData)
	assert.True(t, ok, metric.Name)
	assert.Equal(t, "gauge", metric.MetricType)
	assert.Equal(t, "sys.cpu.user", metric.Name)
}

func TestInvalidOpenTSDBToMetric(t *testing.T) {
	rawData := "put 1356998400 host=webserver01,cpu=0 1"
	c := newOpenTSDB(nil, 12, nil).(*OpenTSDB)
	var conf map[string]interface{}
	c.Configure(conf)
	_, ok := c.parseMetric(rawData)
	assert.False(t, ok)
}

func connectToOpenTSDBCollector(c *OpenTSDB) (net.Conn, error) {
	// emit a metric
	var (
		conn net.Conn
		err  error
	)
	for retry := 0; retry < 3; retry++ {
		if conn, err = net.DialTimeout("tcp", "localhost:"+c.Port(), 2*time.Second); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return conn, err
}
