package collector

import (
	"encoding/json"
	"fullerite/metric"
	"strings"
)

const (
	// MetricTypeCounter String for counter metric type
	MetricTypeCounter = "COUNTER"
	// MetricTypeGauge String for Gauge metric type
	MetricTypeGauge = "GAUGE"
)

func parseDropwizardMetric(raw *[]byte) ([]metric.Metric, error) {
	var parsed map[string]interface{}

	err := json.Unmarshal(*raw, &parsed)

	if err != nil {
		return []metric.Metric{}, err
	}

	metricName := []string{}

	return parseMetricMap(parsed, metricName), nil
}

func parseMetricMap(
	jsonMap map[string]interface{},
	metricName []string) []metric.Metric {
	results := []metric.Metric{}

	for k, v := range jsonMap {
		switch t := v.(type) {
		case map[string]interface{}:
			metricName = append(metricName, k)
			tempResults := parseMetricMap(t, metricName)
			// pop the name, now that it is processed
			if len(metricName)-1 >= 0 {
				metricName = metricName[:(len(metricName) - 1)]
			}
			results = append(results, tempResults...)
		default:
			tempResults := parseFlattenedMetricMap(jsonMap, metricName)
			if len(tempResults) > 0 {
				results = append(results, tempResults...)
				return results
			}
			m, ok := extractGaugeValue(k, v, metricName)
			if ok {
				results = append(results, m)
			}
		}
	}

	return results
}

func extractGaugeValue(key string, value interface{}, metricName []string) (metric.Metric, bool) {
	compositeMetricName := strings.Join(append(metricName, key), ".")
	return createMetricFromDatam("value", value, compositeMetricName, "GAUGE")
}

func parseFlattenedMetricMap(jsonMap map[string]interface{}, metricName []string) []metric.Metric {
	if t, ok := jsonMap["type"]; ok {
		metricType := t.(string)
		if metricType == "gauge" {
			return collectGauge(jsonMap, metricName, "gauge")
		} else if metricType == "histogram" {
			return collectHistogram(jsonMap, metricName, "histogram")
		} else if metricType == "counter" {
			return collectCounter(jsonMap, metricName, "counter")
		} else if metricType == "meter" {
			return collectMeter(jsonMap, metricName)
		}
	}

	// if nothing else works try for rate
	return collectRate(jsonMap, metricName)
}

func collectGauge(jsonMap map[string]interface{}, metricName []string,
	metricType string) []metric.Metric {

	compositeMetricName := strings.Join(metricName, ".")
	return metricFromMap(&jsonMap, compositeMetricName, metricType)
}

func collectHistogram(jsonMap map[string]interface{},
	metricName []string, metricType string) []metric.Metric {

	results := []metric.Metric{}

	if _, ok := jsonMap["count"]; ok {
		for key, value := range jsonMap {
			if key == "type" {
				continue
			}

			metricType := MetricTypeGauge
			if key == "count" {
				metricType = MetricTypeCounter
			}

			compositeMetricName := strings.Join(metricName, ".")
			m, ok := createMetricFromDatam(key, value, compositeMetricName, metricType)
			if ok {
				results = append(results, m)
			}
		}
	}
	return results
}

func collectCounter(jsonMap map[string]interface{}, metricName []string,
	metricType string) []metric.Metric {

	if _, ok := jsonMap["count"]; ok {
		compositeMetricName := strings.Join(metricName, ".")
		return metricFromMap(&jsonMap, compositeMetricName, metricType)
	}
	return []metric.Metric{}
}

func collectRate(jsonMap map[string]interface{}, metricName []string) []metric.Metric {
	results := []metric.Metric{}
	if unit, ok := jsonMap["unit"]; ok && (unit == "seconds" || unit == "milliseconds") {
		for key, value := range jsonMap {
			if key == "unit" {
				continue
			}
			metricType := MetricTypeGauge

			if key == "count" {
				metricType = MetricTypeCounter
			}

			compositeMetricName := strings.Join(metricName, ".")
			m, ok := createMetricFromDatam(key, value, compositeMetricName, metricType)
			if ok {
				results = append(results, m)
			}
		}

	}
	return results
}

func collectMeter(jsonMap map[string]interface{}, metricName []string) []metric.Metric {
	results := []metric.Metric{}

	if checkForMeterUnits(jsonMap) {
		for key, value := range jsonMap {
			if key == "unit" || key == "event_type" || key == "type" {
				continue
			}

			metricType := MetricTypeGauge
			if key == "count" {
				metricType = MetricTypeCounter
			}

			compositeMetricName := strings.Join(metricName, ".")
			m, ok := createMetricFromDatam(key, value, compositeMetricName, metricType)
			if ok {
				results = append(results, m)
			}
		}
	}

	return results
}

func checkForMeterUnits(jsonMap map[string]interface{}) bool {
	if _, ok := jsonMap["event_type"]; ok {
		if unit, ok := jsonMap["unit"]; ok &&
			(unit == "seconds" || unit == "milliseconds" || unit == "minutes") {
			return true
		}
	}
	return false
}
