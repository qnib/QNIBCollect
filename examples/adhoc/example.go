//usr/bin/go run $0 $@; exit
package main

import (
	"encoding/json"
	"fmt"
)

type Metric struct {
	Name       string            `json:"name"`
	Value      float64           `json:"value"`
	Dimensions map[string]string `json:"dimensions"`
	MetricType string            `json:"metricType"`
}

func main() {

	metric := Metric{
		Name:       "test",
		Value:      1.0,
		Dimensions: map[string]string{"dim1": "val1"},
		MetricType: "gauge",
	}
	m, _ := json.Marshal(metric)

	fmt.Println(string(m))

	// OR

	byt := []byte(`{"name":"example","value":6.13,"dimensions":{"dim1","dim2"},"metricType:"cumcounter""}`)
	fmt.Println(string(byt))
}
