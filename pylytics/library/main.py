# Add the project settings module to the namespace.

import argparse
import importlib
import os
import sys

from connection import DB
import settings
from utils import underscore_to_camelcase


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
        '{0}.{1}'.format(dim_or_fact, module_name),
        os.path.dirname(settings.__file__)
        )
    class_name = underscore_to_camelcase(module_name)
    my_class = getattr(module, class_name)
    return my_class


def run_command(facts, command):
    """
    Run command for each fact in facts.

    """
    with DB(settings.pylytics_db) as database_connection:
        for fact in facts:
            sys.stdout.write("Running %s %s\n" % (fact, command))
            MyFact = get_class(fact)(connection=database_connection)
            getattr(MyFact, command)()


def main():
    """This is called by the manage.py created in the project directory."""
    parser = argparse.ArgumentParser(
        description = """
            Run fact scripts.
            e.g.
            > ./manage.py fact_example update
            > ./manage.py all update
            """)
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

    args = parser.parse_args().__dict__
    facts = set(args['fact'])
    command = args['command'][0]

    if 'all' in facts:
        sys.stdout.write('Running all fact scripts:\n')
        facts = all_facts()

    run_command(facts, command)
