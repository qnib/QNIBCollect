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
	handlerInst.SetKeepAliveInterval(config.GetAsInt(globalConfig.KeepAliveInterval, handler.DefaultKeepAliveInterval))
	handlerInst.SetMaxIdleConnectionsPerHost(config.GetAsInt(globalConfig.MaxIdleConnectionsPerHost,
		handler.MaxIdleConnectionsPerHost))

	// now apply the handler level configs
	handlerInst.Configure(instanceConfig)

	go handlerInst.Run()
	return handlerInst
}

func writeToHandlers(handlers []handler.Handler, metric metric.Metric) {
	for _, handler := range handlers {
		handler.Channel() <- metric
	}
}
