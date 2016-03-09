# -*- coding: utf-8 -*-

"""
Collectors Kafka metrics from jolokia agent.

### Example Configuration
"""

from diamond.collector import str_to_bool
from jolokia import JolokiaCollector

class KafkaJolokiaCollector(JolokiaCollector):
    def collect_bean(self, prefix, obj):
        for k, v in obj.iteritems():
            if type(v) in [int, float, long]:
                self.parse_and_publish(prefix, k, v)
            elif isinstance(v, dict):
                self.collect_bean("%s.%s" % (prefix, k), v)
            elif isinstance(v, list):
                self.interpret_bean_with_list("%s.%s" % (prefix, k), v)

    def parse_and_publish(self, prefix, key, value):
        metric_prefix, meta = prefix.split(':', 2)
        name, metric_type, self.dimensions = self.parse_meta(meta)

        metric_name_list = [metric_prefix]
        if self.config.get('prefix', None):
            metric_name_list = [self.config['prefix'], metric_prefix]
        if metric_type:
            metric_name_list.append(metric_type)
        if name:
            metric_name_list.append(name)

        metric_name_list.append(key.lower())
        metric_name = '.'.join(metric_name_list)
        metric_name = self.clean_up(metric_name)
        if metric_name == "":
            self.dimensions = {}
            return

        if key.lower() == 'count':
            self.publish_cumulative_counter(metric_name, value)
        else:
            self.publish(metric_name, value)

    def parse_meta(self, meta):
        dimensions = {}
        for k, v in [kv.split('=') for kv in meta.split(',')]:
            dimensions[str(k)] = v

        metric_name = dimensions.pop("name", None)
        metric_type = dimensions.pop("type", None)
        return metric_name, metric_type, dimensions
