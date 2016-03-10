// +build linux

package collector

import (
	"fullerite/metric"

	"strconv"
	"strings"

	"github.com/prometheus/procfs"
)

// Collect produces some random test metrics.
func (ps *ProcStatus) Collect() {
	counter := 0
	for _, m := range ps.procStatusMetrics() {
		ps.Channel() <- m
		counter++
	}
	ps.metricCounter = uint64(counter)
}

func procStatusPoint(name string, value float64, dimensions map[string]string, metricType string) (m metric.Metric) {
	m = metric.New(name)
	m.Value = value
	m.AddDimensions(dimensions)
	m.MetricType = metricType
	return m
}

func (ps ProcStatus) getMetrics(proc procfs.Proc, cmdOutput []string) []metric.Metric {
	stat, err := proc.NewStat()
	if err != nil {
		ps.log.Warn("Error getting stats: ", err)
		return nil
	}

	pid := strconv.Itoa(stat.PID)

	dim := map[string]string{
		"processName": stat.Comm,
		"pid":         pid,
	}

	ret := []metric.Metric{
		procStatusPoint("VirtualMemory", float64(stat.VirtualMemory()), dim, metric.Gauge),
		procStatusPoint("ResidentMemory", float64(stat.ResidentMemory()), dim, metric.Gauge),
		procStatusPoint("CPUTime", float64(stat.CPUTime()), dim, metric.CumulativeCounter),
	}

	if len(cmdOutput) > 0 {
		generatedDimensions := ps.extractDimensions(cmdOutput[0])
		metric.AddToAll(&ret, generatedDimensions)
	}

	return ret
}

func (ps ProcStatus) procStatusMetrics() []metric.Metric {
	procs, err := procfs.AllProcs()
	if err != nil {
		ps.log.Warn("Error getting processes: ", err)
		return nil
	}

	ret := []metric.Metric{}

	for _, proc := range procs {
		cmd, err := proc.CmdLine()
		if err != nil {
			ps.log.Warn("Error getting command line: ", err)
			continue
		}

		if ps.matches(cmd, proc.Comm) {
			ret = append(ret, ps.getMetrics(proc, cmd)...)
		}
	}

	return ret
}

func (ps ProcStatus) extractDimensions(cmd string) map[string]string {
	ret := map[string]string{}

	for dimension, procRegex := range ps.compiledRegex {
		subMatch := procRegex.FindStringSubmatch(cmd)
		if len(subMatch) > 1 {
			ret[dimension] = subMatch[1]
		}
	}

	return ret
}

func (ps ProcStatus) matches(cmdline []string, comm func() (string, error)) bool {
	var s string
	if ps.matchCommandLine {
		s = strings.Join(cmdline, " ")
	} else {
		comm, err := comm()
		if err != nil {
			ps.log.Warn("Error getting comm: ", err)
			return false
		}
		s = comm
	}

	return ps.pattern.MatchString(s)
}
