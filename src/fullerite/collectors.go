package main

import (
	"fullerite/collector"
	"fullerite/config"
	"fullerite/metric"

	"fmt"
	"strings"
	"time"
)

func startCollectors(c config.Config) (collectors []collector.Collector) {
	log.Info("Starting collectors...")

	for _, name := range c.Collectors {
		configFile := strings.Join([]string{c.CollectorsConfigPath, name}, "/")
		// Since collector naems can be defined with a space in order to instantiate multiple
		// instances of the same collector, we want their files
		// will not have that space and needs to have it replaced with an underscore
		// instead
		configFile = strings.Replace(configFile, " ", "_", -1)
		config, err := config.ReadCollectorConfig(configFile)
		if err != nil {
			log.Error("Collector config failed to load for: ", name)
			continue
		}

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

	ticker := time.NewTicker(time.Duration(collector.Interval()) * time.Second)
	collect := ticker.C

	staggerValue := 1
	collectionDeadline := time.Duration(collector.Interval() + staggerValue)

	for {
		select {
		case <-collect:
			if collector.CollectorType() == "listener" {
				collector.Collect()
			} else {
				countdownTimer := time.AfterFunc(collectionDeadline*time.Second, func() {
					reportCollector(collector)
				})
				collector.Collect()
				countdownTimer.Stop()
			}
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
		if _, exists := metric.GetDimensionValue("collector"); !exists {
			metric.AddDimension("collector", collector.Name())
		}
		metrics <- metric
	}
}

func reportCollector(collector collector.Collector) {
	log.Warn(fmt.Sprintf("%s collector took too long to run, reporting incident!", collector.Name()))
	metric := metric.New("fullerite.collection_time_exceeded")
	metric.Value = 1
	metric.AddDimension("interval", fmt.Sprintf("%d", collector.Interval()))
	collector.Channel() <- metric
}
