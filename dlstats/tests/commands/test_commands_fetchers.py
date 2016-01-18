# -*- coding: utf-8 -*-

from unittest import mock
from click.testing import CliRunner
from dlstats.tests.base import BaseTestCase

class FakeFetcher():
    
    def __init__(self, **kwargs):
        pass

    def datasets_list(self):
        return [{"dataset_code": "dataset1", "name": "dataset 1"}]
    
FETCHERS = {"TEST": FakeFetcher}    

class FetcherNoDBTestCase(BaseTestCase):

    # nosetests -s -v dlstats.tests.commands.test_commands_fetchers:FetcherNoDBTestCase

    def test_list(self):
        runner = CliRunner()
        from dlstats.commands import cmd_fetchers   
        result = runner.invoke(cmd_fetchers.cmd_list, [])
        self.assertEqual(result.exit_code, 0)

    @mock.patch("dlstats.fetchers.FETCHERS", FETCHERS)
    def test_datasets(self):
        runner = CliRunner()
        from dlstats.commands import cmd_fetchers
        result = runner.invoke(cmd_fetchers.cmd_dataset_list, ['-f', 'TEST'])
        self.assertEqual(result.exit_code, 0)
        self.assertTrue("dataset1" in result.output)
