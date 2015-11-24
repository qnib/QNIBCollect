#!/usr/bin/python
# coding=utf-8
################################################################################

from test import CollectorTestCase
from test import get_collector_config
from test import unittest
from mock import Mock
from mock import patch

from diamond.collector import Collector
from osdistro import OSDistroCollector

################################################################################


class TestOSDistroCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('OSDistroCollector', {
        })

        self.collector = OSDistroCollector(config, None)

    def test_import(self):
        self.assertTrue(OSDistroCollector)

    @patch('os.access', Mock(return_value=True))
    @patch.object(Collector, 'publish')
    def test_should_work_with_real_data(self, publish_mock):
        patch_communicate = patch(
            'subprocess.Popen.communicate',
            Mock(return_value=(
                self.getFixture('ubuntu').getvalue(),
                '')))

        patch_communicate.start()
        self.collector.collect()
        patch_communicate.stop()

        self.assertPublishedMany(publish_mock, {
            'os_distro': 'Ubuntu 10.04'
        })

################################################################################
if __name__ == "__main__":
    unittest.main()
