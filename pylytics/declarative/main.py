from datetime import datetime
import inspect
import logging
import sys

import argparse

from pylytics.library import connection
from pylytics.library.log import ColourFormatter, bright_white
from pylytics.declarative.fact import Fact
from pylytics.declarative.warehouse import Warehouse
from pylytics.declarative.settings import Settings, settings


log = logging.getLogger("pylytics")

TITLE = r"""
  _____       _       _   _
 |  __ \     | |     | | (_)
 | |__) |   _| |_   _| |_ _  ___ ___
 |  ___/ | | | | | | | __| |/ __/ __|
 | |   | |_| | | |_| | |_| | (__\__ \
 |_|    \__, |_|\__, |\__|_|\___|___/
         __/ |   __/ |
        |___/   |___/

"""


def get_all_fact_classes():
    """Return all of the fact classes available."""
    fact = __import__('fact')
    public_attributes = [i for i in dir(fact) if not i.startswith('_')]
    facts = []
    for attribute_name in public_attributes:
        attribute =  getattr(fact, attribute_name)
        if inspect.isclass(attribute) and Fact in inspect.getmro(attribute):
            facts.append(attribute)
    return facts


def print_summary(errors):
    """Print out a summary of the errors which happened during run_command."""
    if len(errors) == 0:
        log.debug("No errors raised")
    else:
        log.error("{0} commands not executed: {1}".format(
                  len(errors), ", ".join(errors.keys())))
        items = [
            "- {}: {} {}".format(key, type(value).__name__,
                                 value) for key, value in errors.items()]

        log.info("\n".join(items))


def find_scheduled(all_fact_classes):
    """
    Finds which facts are scheduled to run now.

    Args:
        all_fact_classes:
            A list of all facts available.

    Returns:
        A list of facts which are scheduled to run at the current time.

    """
    # Basic approach now ...
    # Get the current time.
    # Immediately remove any which are outside of the start and end time.
    # The challenge here is timezone awareness ...
    # Can we compare these objects??? Need a load of tests for this ...
    
    
    current_time = None
    
    # TODO 
    
    
    pass


class Commander(object):

    def __init__(self, db_name):
        self.db_name = db_name

    def run(self, command, *facts):
        """ Run command for each fact in facts.
        """
        database = connection.DB(settings.pylytics_db)
        Warehouse.use(database)

        all_fact_classes = get_all_fact_classes()

        # Normalise the collection of facts supplied to remove duplicates,
        # expand "all" and report unknown facts.
        if 'all' in facts:
            facts_to_run = all_fact_classes
        elif 'scheduled' in facts:
            facts_to_run = find_scheduled(all_fact_classes)
        else:
            facts_to_run = []
            fact_names = [type(fact()).__name__ for fact in all_fact_classes]
            for fact_name in facts:
                try:
                    index = fact_names.index(fact_name)
                except ValueError:
                    log.debug('Unrecognised fact %s' % fact_name)
                else:
                    facts_to_run.append(all_fact_classes[index])

            # Remove any duplicates:
            facts_to_run = list(set(facts_to_run))

        # Execute the command on each fact class.
        for fact_class in facts_to_run:
            try:
                command_function = getattr(fact_class, command)
            except AttributeError:
                log.error("Cannot find command %s for fact class %s",
                          command, fact_class)
            else:
                command_function()


def main():
    """ Main function called by the manage.py from the project directory.
    """
    parser = argparse.ArgumentParser(
        description = "Run fact scripts.")
    parser.add_argument(
        '--settings',
        help = 'The path to the settings module e.g /etc/foo/bar',
        type = str,
        nargs = 1,
        )
    parser.add_argument(
        'command',
        help = 'The command you want to run.',
        nargs = 1,
        type = str,
        )
    parser.add_argument(
        'fact',
        help = 'The name(s) of the fact(s) to run e.g. fact_example.',
        nargs = '*',
        type = str,
        )
    args = parser.parse_args().__dict__

    sys.stdout.write(bright_white(TITLE))
    sys.stdout.write(bright_white("\nStarting at {}\n\n".format(
        datetime.now())))

    # Enable log output before loading settings so we have visibility
    # of any load errors.
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ColourFormatter())
    log.addHandler(handler)
    log.setLevel(logging.DEBUG if __debug__ else logging.INFO)

    # Attempt to configure Sentry logging.
    sentry_dsn = settings.SENTRY_DSN
    if sentry_dsn:
        # Only import raven if we're actually going to use it.
        from raven.handlers.logging import SentryHandler
        log.addHandler(SentryHandler(sentry_dsn))

    # Prepend an extra settings file if one is specified.
    settings_module = args["settings"]
    if settings_module:
        settings.prepend(Settings.load(settings_module))

    command = args['command'][0]
    commander = Commander(settings.pylytics_db)

    if command == 'update':
        commander.run('build', *args['fact'])
        commander.run('update', *args['fact'])
    elif command == 'build':
        commander.run('build', *args['fact'])
    else:
        log.error("Unknown command: %s", command)

    sys.stdout.write(bright_white("\nCompleted at {}\n\n".format(
        datetime.now())))
