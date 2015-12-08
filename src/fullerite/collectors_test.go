package main

import (
	"fullerite/config"

	"io/ioutil"
	"os"
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var testFakeConfiguration = `{
    "prefix": "test.",
    "interval": 10,
    "defaultDimensions": {
    },

    "diamondCollectorsPath": "src/diamond/collectors",
    "diamondCollectors": {
    },

    "collectors": {
        "FakeCollector": {
        },
        "Test":{
        }
    },

    "handlers": {
    }
}
`

var (
	tmpTestFakeFile string
)

func TestMain(m *testing.M) {
	logrus.SetLevel(logrus.ErrorLevel)
	if f, err := ioutil.TempFile("/tmp", "fullerite"); err == nil {
		f.WriteString(testFakeConfiguration)
		tmpTestFakeFile = f.Name()
		f.Close()
		defer os.Remove(tmpTestFakeFile)
	}
	os.Exit(m.Run())
}

func TestStartCollectorsEmptyConfig(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)
	collectors := startCollectors(config.Config{})

	assert.NotEqual(t, len(collectors), 1, "should create a Collector")
}

func TestStartCollectorUnknownCollector(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)
	c := make(map[string]interface{})
	collector := startCollector("unknown collector", config.Config{}, c)

	assert.Nil(t, collector, "should NOT create a Collector")
}

func TestStartCollectorsMixedConfig(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)
	conf, _ := config.ReadConfig(tmpTestFakeFile)
	collectors := startCollectors(conf)

	for _, c := range collectors {
		assert.Equal(t, c.Name(), "Test", "Only create valid collectors")
	}
}
