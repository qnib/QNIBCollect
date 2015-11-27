# coding=utf-8

"""
Collect icmp round trip times per hop

#### Dependencies

 * mtr

"""

import diamond.collector
from subprocess import Popen, PIPE


class TracerouteCollector(diamond.collector.ProcessCollector):

    def get_default_config_help(self):
        config_help = super(TracerouteCollector, self).get_default_config_help()
        config_help.update({
            'bin':          "The path to the mtr (Matt's traceroute) binary",
            'hosts':        "Hosts to run the traceroute command on",
            'protocol':     "The protocol to use for the traceroute pings (icmp, udp, tcp)",
            'tcpport':      "The target port number for tcp traces",
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(TracerouteCollector, self).get_default_config()
        config.update({
            'path':     'traceroute',
            'bin':      '/usr/bin/mtr',
            'hosts':    { "yelp":"yelp.com" },
            'protocol': 'icmp',
        })
        return config

    def collect(self):

        for pseudo_hostname, address in self.config.get('hosts', {}).iteritems():

            traceroute = None
            try:
                protocol = self.config.get('protocol', '').lower()
                protocol_args = ''

                if protocol == 'udp':
                    protocol_args = '-u'
                elif protocol == 'tcp':
                    tcpport = self.config.get('tcpport', 80)
                    protocol_args = '-TP {0!s}'.format(tcpport)

                args = ' '.join(['-nrc 1', protocol_args])
                cmd = [self.config['bin'], args, address]

                process = Popen(cmd, stdout=PIPE, stderr=PIPE)
                traceroute, errors = process.communicate()
                if errors:
                    self.log.error(
                        "Error running mtr process: {0!s}".format(errors)
                    )
                    continue
            except Exception as e:
                self.log.error(
                    "Error running TracerouteCollector: {0!s}".format(e)
                )
                continue

            if not traceroute:
                continue

            hop_number = ip = worst = None
            metric_name = '.'.join([
                pseudo_hostname,
                'RoundTripTime',
            ])

            try:
                for hop in traceroute.split('\n')[1:]:
                    # A hop contains: 
                    # hop_number, ip, loss, sent, last, avg, best, worst, stdev
                    # in that order.
                    hop_data = hop.split()

                    if not hop_data or len(hop_data) not in [8, 9]:
                        continue

                    try:
                        [hop_number, ip, _, _, _, _, _, worst, _] = hop_data
                    except ValueError as e:
                        [ip, _, _, _, _, _, worst, _] = hop_data

                    hop_number = hop_number.split('.')[0]
                    rtt = float(worst)

                    self.dimensions = {
                        'hop': hop_number,
                    }

                    if '?' not in ip:
                        self.dimensions['ip'] = ip

                    self.publish(metric_name, rtt)
            except Exception as e:
                self.log.error(
                    "Error publishing metrics: {0}".format(e)
                )
