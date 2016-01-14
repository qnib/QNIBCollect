package main

import (
	"fullerite/config"
	"fullerite/handler"
	"fullerite/metric"
)

func startHandlers(c config.Config) (handlers []handler.Handler) {
	log.Info("Starting handlers...")
	for name, config := range c.Handlers {
		handlers = append(handlers, startHandler(name, c, config))
	}
	return handlers
}

func startHandler(name string, globalConfig config.Config, instanceConfig map[string]interface{}) handler.Handler {
	log.Info("Starting handler ", name)
	handlerInst := handler.New(name)
	if handlerInst == nil {
		return nil
	}

	// apply any global configs
	handlerInst.SetInterval(config.GetAsInt(globalConfig.Interval, handler.DefaultInterval))
	handlerInst.SetPrefix(globalConfig.Prefix)
	handlerInst.SetDefaultDimensions(globalConfig.DefaultDimensions)

	// now apply the handler level configs
	handlerInst.Configure(instanceConfig)

	go handlerInst.Run()
	return handlerInst
}

func writeToHandlers(handlers []handler.Handler, metric metric.Metric) {
	for _, handler := range handlers {
		if canSendMetric(handler, metric) {
			handler.Channel() <- metric
		}
	}
}

func canSendMetric(handler handler.Handler, metric metric.Metric) bool {
	// If the handler's whitelist is set, then only metrics from collectors in it will be emitted. If the same
	// collector is also in the blacklist, it will be skipped.
	// If the handler's whitelist is not set and its blacklist is not empty, only metrics from collectors not in
	// the blacklist will be emitted.
	value, _ := metric.GetDimensionValue("collector")
	isWhiteListed, _ := handler.IsCollectorWhiteListed(value)
	isBlackListed, _ := handler.IsCollectorBlackListed(value)

	// If the handler's whitelist is not nil and not empty, only the whitelisted collectors should be considered
	if handler.CollectorWhiteList() != nil && len(handler.CollectorWhiteList()) > 0 {
		if isWhiteListed && !isBlackListed {
			return true
		}
		return false
	} else {
		// If the handler's whitelist is nil, all collector except the ones in the blacklist are enabled
		if !isBlackListed {
			return true
		}
	}
	return false
}