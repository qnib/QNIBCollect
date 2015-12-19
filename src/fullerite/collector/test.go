package collector

import (
	"fullerite/metric"

	"math/rand"
	"time"

	l "github.com/Sirupsen/logrus"
)

type valueGenerator func() float64

func generateRandomValue() float64 {
	return rand.Float64()
}

// Test collector type
type Test struct {
	baseCollector
	metricName string
	generator  valueGenerator
}

// NewTest creates a new Test collector.
func NewTest(channel chan metric.Metric, initialInterval int, log *l.Entry) *Test {
	t := new(Test)

	t.log = log
	t.channel = channel
	t.interval = initialInterval

	t.name = "Test"
	t.metricName = "TestMetric"
	t.generator = generateRandomValue
	return t
}

// Configure this takes a dictionary of values with which the handler can configure itself
func (t *Test) Configure(configMap map[string]interface{}) {
	if metricName, exists := configMap["metricName"]; exists {
		t.metricName = metricName.(string)
	}
	t.configureCommonParams(configMap)
}

// Collect produces some random test metrics.
func (t Test) Collect() {
	metric := metric.New(t.metricName)
	metric.Value = t.generator()
	metric.AddDimension("testing", "yes")
	t.Channel() <- metric
	t.log.Debug(metric)
	time.Sleep(2 * time.Second)
}
