# coding=utf-8

"""
The OSDistroCollector collects
thedistribution of the OS on a machine.

#### Dependencies

 * /usr/bin/lsb_release

"""

import diamond.collector
import subprocess


class OSDistroCollector(diamond.collector.Collector):

    def collect(self):
        try:
            p = subprocess.Popen(['/usr/bin/lsb_release', '-sir'], stdout=subprocess.PIPE)
            output, errors = p.communicate()
            metric_name = 'os_distro'
            metric_value = output.replace('\n', ' ').strip().strip('"').strip("'")
            self.log.debug("Publishing %s %s" %
                          (metric_name, metric_value))
            self.publish(metric_name, metric_value)
        except Exception as e:
            self.log.error(
                "Failed to get os distro release due to {0!s}".format(e)
            )
