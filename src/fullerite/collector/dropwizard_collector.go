package collector

import (
	"encoding/json"
	"fullerite/metric"
	"strings"
)

const (
	METRIC_TYPE_COUNTER = "COUNTER"
	METRIC_TYPE_GAUGE   = "GAUGE"
)

func parseDropwizardMetric(raw *[]byte) ([]metric.Metric, error) {
	var parsed map[string]interface{}

	err := json.Unmarshal(*raw, &parsed)

	if err != nil {
		return []metric.Metric{}, err
	}

	results := []metric.Metric{}
	metricName := []string{}

	return parseMetricMap(parsed, metricName, results), nil
}

func parseMetricMap(jsonMap map[string]interface{}, metricName []string, results []metric.Metric) []metric.Metric {
	for k, v := range jsonMap {
		switch t := v.(type) {
		case map[string]interface{}:
			metricName = append(metricName, k)
			tempResults := parseMetricMap(t, metricName, results)
			results = append(results, tempResults...)
		default:
			tempResults := parseFlattenedMetricMap(jsonMap, metricName)
			results = append(results, tempResults...)
			return results
		}
	}

	return results
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

func collectGauge(jsonMap map[string]interface{}, metricName []string, metricType string) []metric.Metric {
	compositeMetricName := strings.Join(metricName, ".")
	return metricFromMap(&jsonMap, compositeMetricName, metricType)
}

func collectHistogram(jsonMap map[string]interface{}, metricName []string, metricType string) []metric.Metric {
	if _, ok := jsonMap["count"]; ok {
		compositeMetricName := strings.Join(metricName, ".")
		return metricFromMap(&jsonMap, compositeMetricName, metricType)
	} else {
		return []metric.Metric{}
	}
}

func collectCounter(jsonMap map[string]interface{}, metricName []string, metricType string) []metric.Metric {
	if _, ok := jsonMap["count"]; ok {
		compositeMetricName := strings.Join(metricName, ".")
		return metricFromMap(&jsonMap, compositeMetricName, metricType)
	} else {
		return []metric.Metric{}
	}
}

func collectRate(jsonMap map[string]interface{}, metricName []string) []metric.Metric {
	results := []metric.Metric{}
	if checkForRateUnits(jsonMap) {
		for key, value := range jsonMap {
			if key == "unit" {
				continue
			}
			metricType := METRIC_TYPE_GAUGE

			if key == "count" {
				metricType = METRIC_TYPE_COUNTER
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

			metricType := METRIC_TYPE_GAUGE
			if key == "count" {
				metricType = METRIC_TYPE_COUNTER
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

func checkForRateUnits(jsonMap map[string]interface{}) bool {
	if unit, ok := jsonMap["unit"]; ok && (unit == "seconds" || unit == "milliseconds") {
		return true
	}
	return false
}

func checkForMeterUnits(jsonMap map[string]interface{}) bool {
	if _, ok := jsonMap["event_type"]; ok {
		if unit, ok := jsonMap["unit"]; ok && (unit == "seconds" || unit == "milliseconds" || unit == "minutes") {
			return true
		}
	}
	return false
}
