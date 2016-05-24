package metric

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// The different types of metrics that are supported
const (
	Gauge             = "gauge"
	Counter           = "counter"
	CumulativeCounter = "cumcounter"
)

// Metric type holds all the information for a single metric data
// point. Metrics are generated in collectors and passed to handlers.
type Metric struct {
	Name       string            `json:"name"`
	MetricType string            `json:"type"`
	Value      float64           `json:"value"`
	Dimensions map[string]string `json:"dimensions"`
	Buffered   bool              `json:"buffered"`
	Time       time.Time         `json:"time"`
}

// New returns a new metric with name. Default metric type is "gauge"
// and timestamp is set to now. Value is initialized to 0.0.
func New(name string) Metric {
	return Metric{
		Name:       sanitizeString(name),
		MetricType: "gauge",
		Value:      0.0,
		Dimensions: make(map[string]string),
		Time:       time.Now(),
		Buffered:   false,
	}
}

// NewExt provides a more controled creation
func NewExt(name string, typ string, val float64, d map[string]string, t time.Time, b bool) Metric {
	return Metric{
		Name:       sanitizeString(name),
		MetricType: typ,
		Value:      val,
		Dimensions: d,
		Time:       t,
		Buffered:   b,
	}
}

// Filter provides a struct that can filter a metric by Name (regex), type, dimension (subset of Dimensions)
type Filter struct {
	Name       *regexp.Regexp
	MetricType string
	Dimensions map[string]string
}

// NewFilter returns a Filter with compiled regex
func NewFilter(name string, t string, d map[string]string) Filter {
	return Filter{
		Name:       regexp.MustCompile(name),
		MetricType: t,
		Dimensions: d,
	}
}

// WithValue returns metric with value of type Gauge
func WithValue(name string, value float64) Metric {
	metric := New(name)
	metric.Value = value
	return metric
}

// EnableBuffering puts the metric into buffering handlers (e.g. ZmqBUF)
func (m *Metric) EnableBuffering() {
	m.Buffered = true
}

// DisableBuffering takes the metric out of buffering (e.g. ZmqBUF)
func (m *Metric) DisableBuffering() {
	m.Buffered = false
}

// SetTime to metric
func (m *Metric) SetTime(mtime time.Time) {
	m.Time = mtime
}

// GetTime returns the time
func (m *Metric) GetTime() time.Time {
	return m.Time
}

// AddDimension adds a new dimension to the Metric.
func (m *Metric) AddDimension(name, value string) {
	m.Dimensions[sanitizeString(name)] = sanitizeString(value)
}

// RemoveDimension removes a dimension from the Metric.
func (m *Metric) RemoveDimension(name string) {
	delete(m.Dimensions, name)
}

// AddDimensions adds multiple new dimensions to the Metric.
func (m *Metric) AddDimensions(dimensions map[string]string) {
	for k, v := range dimensions {
		m.AddDimension(k, v)
	}
}

// GetDimensions returns the dimensions of a metric merged with defaults. Defaults win.
func (m *Metric) GetDimensions(defaults map[string]string) (dimensions map[string]string) {
	dimensions = make(map[string]string)
	for name, value := range m.Dimensions {
		dimensions[name] = value
	}
	for name, value := range defaults {
		dimensions[name] = value
	}
	return dimensions
}

// GetDimensionValue returns the value of a dimension if it's set.
func (m *Metric) GetDimensionValue(dimension string) (value string, ok bool) {
	dimension = sanitizeString(dimension)
	value, ok = m.Dimensions[dimension]
	return
}

// ZeroValue is metric zero value
func (m *Metric) ZeroValue() bool {
	return (len(m.Name) == 0) &&
		(len(m.MetricType) == 0) &&
		(m.Value == 0.0) &&
		(len(m.Dimensions) == 0)
}

// AddToAll adds a map of dimensions to a list of metrics
func AddToAll(metrics *[]Metric, dims map[string]string) {
	for _, m := range *metrics {
		for key, value := range dims {
			m.AddDimension(key, value)
		}
	}
}

func sanitizeString(s string) string {
	s = strings.Replace(s, "=", "-", -1)
	s = strings.Replace(s, ":", "-", -1)
	return s
}

// ToJSON Transforms metric to JSON
func (m *Metric) ToJSON() string {
	b, err := json.Marshal(m)
	if err != nil {
		fmt.Println(err)
		return "{}"
	}
	return string(b)
}

// IsSubDim checks if the 2st map contains all items in the second
func (m *Metric) IsSubDim(other map[string]string) bool {
	for k, v := range other {
		val, ok := m.Dimensions[k]
		if !ok || v != val {
			return false
		}
	}
	return true
}

// IsFiltered checks if metrics is filtered with a given filter
func (m *Metric) IsFiltered(f Filter) bool {
	if !m.IsSubDim(f.Dimensions) {
		return false
	}
	if m.MetricType != f.MetricType {
		return false
	}
	if !f.Name.MatchString(m.Name) {
		return false
	}

	return true
}
