# Add the project settings module to the namespace.

import argparse
import importlib
import os
import sys

from utils.text_conversion import underscore_to_camelcase
from utils.terminal import print_status


def all_facts():
    """Return the names of all facts in the facts folder."""
    fact_filenames = os.listdir('fact')
    facts = []
    for filename in fact_filenames:
        if filename.startswith('fact_') and filename.endswith('.py'):
            facts.append(filename.split('.')[0])
    return facts


def get_class(module_name, dimension=False):
    """
    Effectively does this:
    from fact/fact_count_all_the_sales import FactCountAllTheSales
    return FactCountAllTheSales

    Example usage:
    get_class('fact_count_all_the_sales')

    If dimension is True then it searches for a dimension instead.

    """
    if dimension:
        dim_or_fact = 'dim'
    else:
        dim_or_fact = 'fact'
    
    module = importlib.import_module(
        '{0}.{1}'.format(dim_or_fact, module_name)
        )
    class_name = underscore_to_camelcase(module_name)
    my_class = getattr(module, class_name)
    return my_class


def print_summary(errors):
    """Print out a summary of the errors which happened during run_command."""
    print_status("Summary", format='reverse', space=True, timestamp=False,
                 indent=False)
    if len(errors) == 0:
        print_status("Everything went fine!", format='green', timestamp=False,
                     indent=False)
    else:
        print_status("{0} commands not executed: {1}".format(
                len(errors),
                ", ".join(errors.keys())
                ),
            timestamp=False,
            indent=False
            )
        template = "- {0}: {1} {2}"
        items = [template.format(key, type(value).__name__, value) for key, value in errors.items()]
        print_status(
            "\n".join(items),
            timestamp=False,
            indent=False
            )


def run_command(facts, command):
    """
    Run command for each fact in facts.

    """
    from connection import DB
    errors = {}
    
    with DB(settings.pylytics_db) as database_connection:
        for fact in facts:
            print_status("Running {0} {1}".format(fact, command),
                         format='blue', indent=False, space=True,
                         timestamp=False)
            try:
                MyFact = get_class(fact)(connection=database_connection)
                getattr(MyFact, command)()
            except Exception as e:
                print_status("Running {0} {1} failed!".format(fact, command),
                             format='red')
                errors['.'.join([fact, command])] = e
    
    print_summary(errors)


def load_settings(settings_path):
    if settings_path:
        if settings_path[0]:
            sys.path.insert(0, settings_path)
    global settings
    import settings


def main():
    """This is called by the manage.py created in the project directory."""
    parser = argparse.ArgumentParser(
        description = "Run fact scripts.")
    parser.add_argument(
        'fact',
        choices = ['all'] + all_facts(),
        help = 'The name(s) of the fact(s) to run e.g. fact_example.',
        nargs = '+',
        type = str,
        )
    parser.add_argument(
        'command',
        choices = ['update', 'build', 'test', 'historical'],
        help = 'The command you want to run.',
        nargs = 1,
        type = str,
        )
    parser.add_argument(
        '--settings',
        help = 'The path to the settings module e.g /etc/foo/bar',
        type = str,
        nargs = 1,
        )
    
    args = parser.parse_args().__dict__
    facts = set(args['fact'])
    command = args['command'][0]
    
    if 'all' in facts:
        print_status('Running all fact scripts:', indent=False,
                     timestamp=False, format='reverse', space=True)
        facts = all_facts()
    
    # Import settings:
    load_settings(args['settings'])

    run_command(facts, command)
