# coding=utf-8

"""
Port of the ganglia gearman collector
Collects stats from gearman job server

#### Dependencies

 *  gearman

"""

import diamond.collector
import os
import subprocess
import time

try:
    import gearman
except ImportError:
    gearman = None


class GearmanCollector(diamond.collector.Collector):

    def get_default_config_help(sef):
        config_help = super(GearmanCollector, self).get_default_config_help()
        config_help.update({
            'gearman_pid_path': 'Gearman PID file path',
            'url': 'Gearman endpoint to talk to',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(GearmanCollector, self).get_default_config()
        config.update({
            'path': 'gearman_stats',
            'gearman_pid_path': '/var/run/gearman/gearman-job-server.pid',
            'url': 'localhost',
        })
        return config

    def collect(self):
        """
        Collector gearman stats
        """
        def gearman_ping(gm_admin_client):
            return gm_admin_client.ping_server()

        def gearman_queued(gm_admin_client):
            return sum(entry['queued'] 
                    for entry in gm_admin_client.get_status())

        def get_fds(gearman_pid_path):
            with open(gearman_pid_path) as fp:
                gearman_pid = fp.read().strip()
            proc_path = os.path.join('/proc', gearman_pid, 'fd')
            return len(os.listdir(proc_path))

        try:
            if gearman is None:
                self.log.error("Unable to import python gearman client")
                return

            # Collect and Publish Metrics
            self.log.debug("Using pid file: %s & gearman endpoint : %s",
                    self.config['gearman_pid_path'], self.config['url'])
            gm_admin_client = gearman.GearmanAdminClient([self.config['url']])
            self.publish('gearman.ping', gearman_ping(gm_admin_client))
            self.publish('gearman.queued', gearman_queued(gm_admin_client))
            self.publish('gearman.fds', get_fds(self.config['gearman_pid_path']))
        except Exception, e:
            self.log.error("GearmanCollector Error: %s", e)
