package main

import (
	"fullerite/config"
	"fullerite/handler"
	"fullerite/metric"

	"fmt"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestStartHandlersEmptyConfig(t *testing.T) {
	logrus.SetLevel(logrus.PanicLevel)
	handlers := startHandlers(config.Config{})

	assert.Zero(t, len(handlers), "should not create any Handler")
}

func TestStartHandlerUnknownHandler(t *testing.T) {
	logrus.SetLevel(logrus.PanicLevel)

	c := make(map[string]interface{})
	handler := startHandler("unknown handler", config.Config{}, c)

	assert.Nil(t, handler)
}

func checkEmission(t *testing.T, coll string, h handler.Handler, expected bool) {
	m := metric.Metric{
		Name:       "test",
		Value:      1,
		Dimensions: map[string]string{"collector": coll},
	}
	writeToHandlers([]handler.Handler{h}, m)

	select {
	case res := <-h.Channel():
		if !expected {
			assert.Fail(t, fmt.Sprintf("Did not expect metric %s", res.Name))
		}
	default:
		if expected {
			assert.Fail(t, "Was expecting the metric to go through")
		}
	}
}

func TestCanSendMetricsWhiteList(t *testing.T) {
	logrus.SetLevel(logrus.PanicLevel)

	channel := make(chan metric.Metric, 5)
	timeout := time.Duration(5 * time.Second)
	log := logrus.WithFields(logrus.Fields{"app": "fullerite", "pkg": "handler"})
	h := handler.NewSignalFx(channel, 10, 10, timeout, log)

	h.SetCollectorWhiteList([]string{"coll1", "coll2"})
	h.SetCollectorBlackList([]string{"coll2"})

	checkEmission(t, "coll1", h, true)
	checkEmission(t, "coll2", h, false)
	checkEmission(t, "coll3", h, false)
}

func TestCanSendMetricsOnlyBlackList(t *testing.T) {
	logrus.SetLevel(logrus.PanicLevel)

	channel := make(chan metric.Metric, 5)
	timeout := time.Duration(5 * time.Second)
	log := logrus.WithFields(logrus.Fields{"app": "fullerite", "pkg": "handler"})
	h := handler.NewSignalFx(channel, 10, 10, timeout, log)

	h.SetCollectorBlackList([]string{"coll2"})

	checkEmission(t, "coll1", h, true)
	checkEmission(t, "coll2", h, false)
	checkEmission(t, "coll3", h, true)
}

func TestCanSendMetrics(t *testing.T) {
	logrus.SetLevel(logrus.PanicLevel)

	channel := make(chan metric.Metric, 5)
	timeout := time.Duration(5 * time.Second)
	log := logrus.WithFields(logrus.Fields{"app": "fullerite", "pkg": "handler"})
	h := handler.NewSignalFx(channel, 10, 10, timeout, log)

	checkEmission(t, "coll1", h, true)
	checkEmission(t, "coll2", h, true)
	checkEmission(t, "coll3", h, true)
}
