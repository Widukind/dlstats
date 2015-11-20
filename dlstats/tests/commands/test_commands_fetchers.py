# -*- coding: utf-8 -*-

from dlstats.fetchers import FETCHERS, FETCHERS_DATASETS
from dlstats.commands import cmd_fetchers

import unittest
from click.testing import CliRunner
from dlstats.tests.base import BaseTestCase, BaseDBTestCase, RESOURCES_DIR

class FakeFetcher():
    
    def __init__(self, **kwargs):
        pass

    def datasets_list(self):
        return ["dataset1"]

    def datasets_long_list(self):
        return [("dataset1", "Dataset1")]

class FetcherNoDBTestCase(BaseTestCase):

    # nosetests -s -v dlstats.tests.commands.test_commands_fetchers:FetcherNoDBTestCase

    def setUp(self):
        BaseTestCase.setUp(self)
        #TODO: mock ?
        self.backup_FETCHERS = FETCHERS.copy()
        FETCHERS["TEST"] = FakeFetcher

    def tearDown(self):
        BaseTestCase.tearDown(self)
        FETCHERS = self.backup_FETCHERS
        
    def test_list(self):
        runner = CliRunner()   
        result = runner.invoke(cmd_fetchers.cmd_list, [])
        self.assertEqual(result.exit_code, 0)

    def test_datasets(self):
        runner = CliRunner()
        result = runner.invoke(cmd_fetchers.cmd_dataset_list, ['-f', 'TEST'])
        self.assertEqual(result.exit_code, 0)
        self.assertTrue("dataset1" in result.output)
