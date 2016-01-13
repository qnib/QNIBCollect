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
	// whitelist and blacklist are mutually exclusive: you cannot set both for a single handler!
	// If the handler's whitelist is set, then only metrics from collectors in it will be emitted.
	// If the handler's whitelist is not set and its blacklist is not empty, only metrics from collectors not in
	// the blacklist will be emitted.
	for _, handler := range handlers {
		value, ok := metric.GetDimensionValue("collector")

		// If the handler's whitelist is not nil, only the whitelisted collectors should be considered
		if handler.CollectorWhiteList() != nil {
			isWhiteListed, _ := handler.IsCollectorWhiteListed(value)
			if ok && isWhiteListed {
				handler.Channel() <- metric
			}
		} else {

			// If the handler's whitelist is nil, all collector except the ones in the blacklist are enabled
			isBlackListed, _ := handler.IsCollectorBlackListed(value)
			if ok && isBlackListed {
				// This collector is black listed by
				// this handler. Therefore we are dropping this
				log.Debug("Not forwarding metrics from", value, "collector to", handler.Name(), "handler, since it has blacklisted this collector")
			} else {
				handler.Channel() <- metric
			}
		}
	}
}
