# /usr/bin/env python
"""
Module to test main module
"""
from __future__ import print_function
from __future__ import division

import unittest
from mock import patch, PropertyMock
from main import App

# pylint: disable=R0201
class TestMain(unittest.TestCase):
    """
    Class to test main module
    """

    def test_attributes(self):
        """
        test case to validate attributes
        :return:
        """
        print("------------validating attributes------------------")
        assert hasattr(App, 'cogo_labs_df')
        assert hasattr(App, 'join_df')
        assert hasattr(App, 'live_works_df')
        assert hasattr(App, 'persist')
        assert hasattr(App, 'run')
        assert isinstance(App.cogo_labs_df, property)
        assert isinstance(App.live_works_df, property)

    @patch('main.generate_cogo_labs_data')
    def test_generate_cogo_labs_data(self, generate_cogo_labs_data=None):
        """
        test case to validate generate_cogo_labs_data function
        :param generate_cogo_labs_data:
        :return:
        """
        print("------------validating generate_cogo_labs_data------------------")
        generate_cogo_labs_data.return_value = 'test'
        assert generate_cogo_labs_data() == 'test'

    @patch('main.App')
    def test_app(self, App=None):
        """
        test case to validate app instantiation
        :param App:
        :return:
        """
        print("------------validating instantiation of App------------------")
        App.return_value = 'app test'
        assert App() == 'app test'

    @patch('main.App.cogo_labs_df', new_callable=PropertyMock)
    def test_cogo_labs_df(self, cogo_labs_df=None):
        """
        test case to validate cogo_labs data frame property
        :param cogo_labs_df:
        :return:
        """
        print("------------validating Cogo labs dataframe property------------------")
        cogo_labs_df.return_value = 'mock cogo labs dataframe'
        t = App()
        assert t.cogo_labs_df == 'mock cogo labs dataframe'

    @patch('main.App.live_works_df', new_callable=PropertyMock)
    def test_live_works_df(self, live_works_df=None):
        """
        test case to validate live works data frame property
        :param live_works_df:
        :return:
        """
        print("------------validating live works dataframe property------------------")
        live_works_df.return_value = 'mock live works dataframe'
        t = App()
        assert t.live_works_df == 'mock live works dataframe'

    @patch('main.App.join_df', new_callable=PropertyMock)
    def test_join_df(self, join_df=None):
        """
        test case to validate join dataframe property
        :param join_df:
        :return:
        """
        print("------------validating join dataframe property------------------")
        join_df.return_value = 'mock join dataframe'
        t = App()
        assert t.join_df == 'mock join dataframe'

    @patch('main.App.run')
    def test_run(self, run=None):
        """
        test case to validate run function
        :param run:
        :return:
        """
        print("------------validating run function------------------")
        run.return_value = 'test run'
        t = App()
        assert t.run() == 'test run'

    @patch('main.App.persist')
    def test_persist(self, persist=None):
        """
        test case to validate persist function
        :param persist:
        :return:
        """
        print("------------validating persist function------------------")
        persist.return_value = 'test persist'
        t = App()
        assert t.persist() == 'test persist'


if __name__ == '__main__':
    test = TestMain()
    test.test_app()
    test.test_generate_cogo_labs_data()
    test.test_attributes()
    test.test_cogo_labs_df()
    test.test_live_works_df()
    test.test_run()
    test.test_persist()
    test.test_join_df()
