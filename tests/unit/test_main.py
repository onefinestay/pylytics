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

        importlib.import_module.assert_called_with('fact.fact_example')
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

        importlib.import_module.assert_called_with('dim.dim_example')
        assert response.name == 'DimExample'


class TestProcessScripts(object):

    @patch('pylytics.library.main.importlib')
    def test_executed(self, importlib):
        script = Mock()
        importlib.import_module.return_value = script
        main._process_scripts(['my_script'])

        importlib.import_module.assert_called_with('scripts.my_script')
        script.assert_called('main')


class TestExtractScripts(object):

    test_commands = ['update', 'historical', 'build', 'drop']

    def _get_mock_fact(self):
        TestFact = Mock()
        TestFact.setup_scripts = {}
        for command in self.test_commands:
            TestFact.setup_scripts[command] = ['test_{}'.format(command)]

        return TestFact

    def test_response(self):
        fact_classes = [self._get_mock_fact()]
        for command in self.test_commands:
            script_list = main._extract_scripts(command, fact_classes)
            assert script_list == ['test_{}'.format(command)]

    def test_no_duplicates(self):
        fact_classes = [self._get_mock_fact(), self._get_mock_fact()]
        for command in self.test_commands:
            script_list = main._extract_scripts(command, fact_classes)
            assert script_list == ['test_{}'.format(command)]


# TODO - tests for run_command. Will probably have to refactor run_command
# first.
