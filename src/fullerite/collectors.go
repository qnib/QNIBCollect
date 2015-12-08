package main

import (
	"fullerite/collector"
	"fullerite/config"
	"fullerite/metric"
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
	for {
		collector.Collect()
		time.Sleep(time.Duration(collector.Interval()) * time.Second)
	}
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
