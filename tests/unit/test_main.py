from mock import Mock, patch
import pytest

from pylytics.library import main


class TestAllFacts(object):

    @patch('pylytics.library.main.os')
    def test_no_garbage_files(self, os):
        os.listdir.return_value = ['fact_example.py', 'some_file.txt',
                                   'fact_example_1', 'fact_old_example.pyc']
        assert main.all_facts() == ['fact_example']


@patch('pylytics.library.main.importlib')
class TestGetClass(object):

    def test_fact(self, importlib):
        """
        Make sure that FactExample gets accessed and returned from
        fact_example.

        """
        fact_example = Mock()
        fact_example.FactExample.name = 'FactExample'
        importlib.import_module.return_value = fact_example

        response = main.get_class('fact_example')
        assert response.name == 'FactExample'

    def test_dim(self, importlib):
        """
        Make sure that DimExample gets accessed and returned from
        dim_example.

        """
        dim_example = Mock()
        dim_example.DimExample.name = 'DimExample'
        importlib.import_module.return_value = dim_example

        response = main.get_class('dim_example', dimension=True)
        assert response.name == 'DimExample'
