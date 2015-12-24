package collector

import (
	"fmt"
	"os"

	"fullerite/metric"

	"github.com/Sirupsen/logrus"
)

// LogErrorHook to send errors via collector channel.
type LogErrorHook struct {
	collectorChannel chan metric.Metric

	// intentionally exported
	log *logrus.Entry
}

// NewLogErrorHook creates a hook to be added to the collector logger
// so that errors are forwards as a metric to the collecot
// channel.
func NewLogErrorHook(collectorChannel chan metric.Metric) *LogErrorHook {
	hookLog := defaultLog.WithFields(logrus.Fields{"hook": "LogErrorHook"})
	return &LogErrorHook{collectorChannel, hookLog}
}

// Fire action to take when log is fired.
func (hook *LogErrorHook) Fire(entry *logrus.Entry) error {
	_, err := entry.String()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read entry, %v", err)
		return err
	}

	switch entry.Level {
	case logrus.ErrorLevel:
		go hook.reportErrors()
	case logrus.FatalLevel:
		go hook.reportErrors()
	case logrus.PanicLevel:
		go hook.reportErrors()
	default:
	}
	return nil
}

// Levels covered by this hook
func (hook *LogErrorHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
	}
}

func (hook *LogErrorHook) reportErrors() {
	metric := metric.New("fullerite.collector_errors")
	metric.Value = 1
	hook.collectorChannel <- metric
	return
}
