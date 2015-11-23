package collector

import (
	"fullerite/metric"
	"testing"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestMesosStatsNewMesosStats(t *testing.T) {
	c := make(chan metric.Metric)
	i := 10
	l := defaultLog.WithFields(l.Fields{"collector": "Mesos"})

	sut := NewMesosStats(c, i, l)

	assert.Equal(t, c, sut.channel)
	assert.Equal(t, i, sut.interval)
	assert.Equal(t, l, sut.log)
}

func TestMesosStatsConfigure(t *testing.T) {
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
		sut.Configure(config)

		switch test.isNil {
		case true:
			assert.Nil(t, sut.mesosCache, test.msg)
		case false:
			assert.NotNil(t, sut.mesosCache, test.msg)
		}

	}
}
