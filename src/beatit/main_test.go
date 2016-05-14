package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateMetrics(t *testing.T) {
	dps := 10
	numMetrics := 10
	metrics := generateMetrics("test", numMetrics, dps, false)
	assert.Equal(t, len(metrics), dps)
}

func TestGenerateMetricsMoreMetricsThanDPS(t *testing.T) {
	dps := 5
	numMetrics := 10
	metrics := generateMetrics("test", numMetrics, dps, false)
	assert.Equal(t, len(metrics), dps)
}
