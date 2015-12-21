package main

import (
	"fullerite/collector"
	"fullerite/config"
	"fullerite/metric"

	"fmt"
	"time"
)

func startCollectors(c config.Config) (collectors []collector.Collector) {
	log.Info("Starting collectors...")
	for name, config := range c.Collectors {
		collectorInst := startCollector(name, c, config)
		if collectorInst != nil {
			collectors = append(collectors, collectorInst)
		}
	}
	return collectors
}

func startCollector(name string, globalConfig config.Config, instanceConfig map[string]interface{}) collector.Collector {
	log.Debug("Starting collector ", name)
	collectorInst := collector.New(name)
	if collectorInst == nil {
		return nil
	}

	// apply the global configs
	collectorInst.SetInterval(config.GetAsInt(globalConfig.Interval, collector.DefaultCollectionInterval))

	// apply the instance configs
	collectorInst.Configure(instanceConfig)

	go runCollector(collectorInst)
	return collectorInst
}

func runCollector(collector collector.Collector) {
	log.Info("Running ", collector)

	var listen <-chan time.Time
	var collect <-chan time.Time

	ticker := time.NewTicker(time.Duration(collector.Interval()) * time.Second)
	if collector.CollectorType() == "listener" {
		listen = ticker.C
	} else {
		collect = ticker.C
	}

	staggerValue := 1
	collectionDeadline := time.Duration(collector.Interval() + staggerValue)

	for {
		select {
		case <-listen:
			collector.Collect()
		case <-collect:
			countdownTimer := time.AfterFunc(collectionDeadline*time.Second, func() {
				reportCollector(collector)
			})
			collector.Collect()
			countdownTimer.Stop()
		}
	}
	ticker.Stop()
}

func readFromCollectors(collectors []collector.Collector, metrics chan metric.Metric) {
	for _, collector := range collectors {
		go readFromCollector(collector, metrics)
	}
}

func readFromCollector(collector collector.Collector, metrics chan metric.Metric) {
	for metric := range collector.Channel() {
		metrics <- metric
	}
}

func reportCollector(collector collector.Collector) {
	log.Error(fmt.Sprintf("%s collector took too long to run, reporting incident!", collector.Name()))
	metric := metric.New("fullerite.collection_time_exceeded")
	metric.Value = 1
	metric.AddDimension("interval", fmt.Sprintf("%d", collector.Interval()))
	collector.Channel() <- metric
}
