#!/usr/bin/env python
# encoding: utf-8
"""This is the main entry point for running analytics scripts."""

import argparse
import sys

from pylytics.library.connection import DB
from pylytics.library.utils import all_facts, underscore_to_camelcase

import settings


def get_class(module, dimension=False):
    """
    Effectively does this:
    from fact/fact_count_allthesales import FactCountAllTheSales
    return FactCountAllTheSales

    Example usage:
    get_class('fact_count_allthesales')

    If dimension is True then it searches for a dimension instead.

    """
    if dimension:
        dim_or_fact = 'dim'
    else:
        dim_or_fact = 'fact'

    df_module = __import__('%s.%s' % (dim_or_fact, module), globals(),
                           locals(), [dim_or_fact])
    class_name = underscore_to_camelcase(module)
    my_class = getattr(df_module, class_name)
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


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description = """
            Run fact scripts.
            e.g.
            > ./manage.py fact_count_allthesales update
            > ./manage.py all update
            """)
    parser.add_argument(
        'fact',
        choices = ['all'] + all_facts(),
        help = 'The name(s) of the fact(s) to run e.g. fact_count_allthesales',
        nargs = '+',
        type = str,
        )
    parser.add_argument(
        'command',
        choices = ['update', 'build','test'],
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
