#!/usr/bin/python
# coding=utf-8
#######################################################################

from test import CollectorTestCase
from test import get_collector_config
from test import run_only
from test import unittest
from mock import Mock
from mock import patch

import os

from diamond.collector import Collector
from gearman_stats import GearmanCollector
#######################################################################


def run_only_if_gearman_is_available(func):
    try:
        import gearman
    except ImportError:
        gearman = None
    pred = lambda: gearman is not None
    return run_only(func, pred)


class TestGearmanCollector(CollectorTestCase):

    def setUp(self):
        config = get_collector_config('GearmanCollector', {})
        self.collector = GearmanCollector(config, None)

        #  Use a dummy pid file for testing.
        mock_pid_path = os.path.dirname(__file__) + '/fixtures/gearman_dummy_pid'
        self.collector.config['gearman_pid_path'] = mock_pid_path

    def test_import(self):
        self.assertTrue(GearmanCollector)

    @run_only_if_gearman_is_available
    @patch('gearman.GearmanAdminClient')
    @patch('os.listdir', Mock(return_value=[1,2,3]))
    @patch.object(Collector, 'publish')
    def test_collect(self, publish_mock, gearman_mock):

        #  Setup mocks
        client = Mock()
        ping_server_mock_return = 0.1
        gearman_stats_mock_return = [
            {"queued": 5},
            {"queued": 5},
        ]

        gearman_mock.return_value = client
        client.ping_server.return_value = ping_server_mock_return
        client.get_status.return_value = gearman_stats_mock_return
        
        self.collector.collect()

        metrics = {
            "gearman_ping": ping_server_mock_return,
            "gearman_queued": sum(entry['queued'] for entry in gearman_stats_mock_return),
            "gearman_fds": 3,
        }

        self.setDocExample(collector=self.collector.__class__.__name__,
                metrics=metrics,
                defaultpath=self.collector.config['path'])
        self.assertPublishedMany(publish_mock, metrics)

    
    @run_only_if_gearman_is_available
    @patch('gearman.GearmanAdminClient')
    @patch.object(Collector, 'publish')
    def test_fail_gracefully(self, publish_mock, gearman_mock):
        
        #  Setup mocks
        client = Mock()
        gearman_mock.return_value = client
        client.ping_server.side_effect = IOError()

        self.collector.collect()

        self.assertPublishedMany(publish_mock, {})
    

#######################################################################
if __name__ == "__main__":
    unittest.main()
