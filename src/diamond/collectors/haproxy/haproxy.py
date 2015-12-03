# coding=utf-8

"""
Collect HAProxy Stats

#### Dependencies

 * urlparse
 * urllib2

haproxy?stats returns:
 act: server is active (server), number of active servers (backend)
 bck: server is backup (server), number of backup servers (backend)
 bin: bytes in
 bout: bytes out
 check_code: layer5-7 code, if available
 check_duration: time in ms took to finish last health check
 check_status: status of last health check, one of:
        UNK     -> unknown
        INI     -> initializing
        SOCKERR -> socket error
        L4OK    -> check passed on layer 4, no upper layers testing enabled
        L4TMOUT -> layer 1-4 timeout
        L4CON   -> layer 1-4 connection problem, for example
                   "Connection refused" (tcp rst) or "No route to host" (icmp)
        L6OK    -> check passed on layer 6
        L6TOUT  -> layer 6 (SSL) timeout
        L6RSP   -> layer 6 invalid response - protocol error
        L7OK    -> check passed on layer 7
        L7OKC   -> check conditionally passed on layer 7, for example 404 with
                   disable-on-404
        L7TOUT  -> layer 7 (HTTP/SMTP) timeout
        L7RSP   -> layer 7 invalid response - protocol error
        L7STS   -> layer 7 response error, for example HTTP 5xx
 chkdown: number of UP->DOWN transitions
 chkfail: number of failed checks
 cli_abrt: number of data transfers aborted by the client
 downtime: total downtime (in seconds)
 dreq: denied requests
 dresp: denied responses
 econ: connection errors
 ereq: request errors
 eresp: response errors (among which srv_abrt)
 hanafail: failed health checks details
 hrsp_1xx: http responses with 1xx code
 hrsp_2xx: http responses with 2xx code
 hrsp_3xx: http responses with 3xx code
 hrsp_4xx: http responses with 4xx code
 hrsp_5xx: http responses with 5xx code
 hrsp_other: http responses with other codes (protocol error)
 iid: unique proxy id
 lastchg: last status change (in seconds)
 lbtot: total number of times a server was selected
 pid: process id (0 for first instance, 1 for second, ...)
 pxname: proxy name
 qcur: current queued requests
 qlimit: queue limit
 qmax: max queued requests
 rate_lim: limit on new sessions per second
 rate_max: max number of new sessions per second
 rate: number of sessions per second over last elapsed second
 req_rate: HTTP requests per second over last elapsed second
 req_rate_max: max number of HTTP requests per second observed
 req_tot: total number of HTTP requests received
 scur: current sessions
 sid: service id (unique inside a proxy)
 slim: sessions limit
 smax: max sessions
 srv_abrt: number of data transfers aborted by the server (inc. in eresp)
 status: status (UP/DOWN/NOLB/MAINT/MAINT(via)...)
 stot: total sessions
 svname: service name (FRONTEND for frontend, BACKEND for backend, any name for server)
 throttle: warm up status
 tracked: id of proxy/server if tracking is enabled
 type (0=frontend, 1=backend, 2=server, 3=socket)
 weight: server weight (server), total weight (backend)
 wredis: redispatches (warning)
 wretr: retries (warning)
"""

import re
import urllib2
import base64
import csv
import diamond.collector


class HAProxyCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(HAProxyCollector, self).get_default_config_help()
        config_help.update({
            'url': "Url to stats in csv format",
            'user': "Username",
            'pass': "Password",
            'ignore_servers': "Ignore servers, just collect frontend and "
                              + "backend stats",
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(HAProxyCollector, self).get_default_config()
        config.update({
            'path':             'haproxy',
            'url':              'http://localhost/haproxy?stats;csv',
            'user':             'admin',
            'pass':             'password',
            'ignore_servers':   False,
        })
        return config

    def _get_config_value(self, section, key):
        if section:
            if section not in self.config:
                self.log.error("Error: Config section '%s' not found", section)
                return None
            return self.config[section].get(key, self.config[key])
        else:
            return self.config[key]

    def get_csv_data(self, section=None):
        """
        Request stats from HAProxy Server
        """
        metrics = []
        req = urllib2.Request(self._get_config_value(section, 'url'))
        try:
            handle = urllib2.urlopen(req)
            return handle.readlines()
        except Exception, e:
            if not hasattr(e, 'code') or e.code != 401:
                self.log.error("Error retrieving HAProxy stats. %s", e)
                return metrics

        # get the www-authenticate line from the headers
        # which has the authentication scheme and realm in it
        authline = e.headers['www-authenticate']

        # this regular expression is used to extract scheme and realm
        authre = (r'''(?:\s*www-authenticate\s*:)?\s*'''
                  + '''(\w*)\s+realm=['"]([^'"]+)['"]''')
        authobj = re.compile(authre, re.IGNORECASE)
        matchobj = authobj.match(authline)
        if not matchobj:
            # if the authline isn't matched by the regular expression
            # then something is wrong
            self.log.error('The authentication header is malformed.')
            return metrics

        scheme = matchobj.group(1)
        # here we've extracted the scheme
        # and the realm from the header
        if scheme.lower() != 'basic':
            self.log.error('Invalid authentication scheme.')
            return metrics

        base64string = base64.encodestring(
            '%s:%s' % (self._get_config_value(section, 'user'),
                       self._get_config_value(section, 'pass')))[:-1]
        authheader = 'Basic %s' % base64string
        req.add_header("Authorization", authheader)
        try:
            handle = urllib2.urlopen(req)
            metrics = handle.readlines()
            return metrics
        except IOError, e:
            # here we shouldn't fail if the USER/PASS is right
            self.log.error("Error retrieving HAProxy stats. (Invalid username "
                           + "or password?) %s", e)
            return metrics

    def _generate_headings(self, row):
        headings = {}
        for index, heading in enumerate(row):
            headings[index] = self._sanitize(heading)
        return headings

    def _collect(self, section=None):
        """
        Collect HAProxy Stats
        """
        csv_data = self.get_csv_data(section)
        data = list(csv.reader(csv_data))
        headings = self._generate_headings(data[0])
        section_name = section and self._sanitize(section.lower()) + '.' or ''

        for row in data:
            if (self._get_config_value(section, 'ignore_servers')
                    and row[1].lower() not in ['frontend', 'backend']):
                continue
            proxy_name = self._sanitize(row[0].lower())
            server_name = self._sanitize(row[1].lower())

            for index, metric_string in enumerate(row):
                try:
                    metric_value = float(metric_string)
                except ValueError:
                    continue

                self.dimensions = {
                    'proxy_name': proxy_name,
                    'server_name': server_name,
                }
                metric_name = '.'.join(['haproxy', headings[index]])
                if section_name:
                    metric_name = '.'.join([section_name, metric_name])
                self.publish(metric_name, metric_value, metric_type='GAUGE')

    def collect(self):
        if 'servers' in self.config:
            if isinstance(self.config['servers'], list):
                for serv in self.config['servers']:
                    self._collect(serv)
            else:
                self._collect(self.config['servers'])
        else:
            self._collect()

    def _sanitize(self, s):
        """Sanitize the name of a metric to remove unwanted chars
        """
        return re.sub('[^\w-]', '_', s)
