#!/usr/bin/python
# coding=utf-8
################################################################################

from mock import patch
from test import unittest
import configobj

from diamond.collector import Collector


class BaseCollectorTest(unittest.TestCase):

    @patch('diamond.collector.Collector.publish_metric')
    def test_SetDimensions(self, mock_publish):
        """
        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}
        c = Collector(config, [])
        c.dimensions = {
            'metric1': {'dim1':'alice'},
            'metric2': {'dim2':'bob', 'foo':'bar'}
        }
        c.publish('metric1', 1)
        c.publish('bat', 2)

        for call in mock_publish.mock_calls:
            name, args, kwargs = call
            metric = args[0]
            self.assertEquals(metric.dimensions, c.dimensions[metric.name])
        """
        pass
