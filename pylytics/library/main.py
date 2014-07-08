from datetime import datetime
import logging
import sys

import argparse
import importlib
import os
from pylytics.library.log import ColourFormatter, bright_white
from pylytics.library.table import Table

from utils.text_conversion import underscore_to_camelcase


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


def all_facts():
    """Return the names of all facts in the facts folder."""
    fact_filenames = os.listdir('fact')
    facts = []
    for filename in fact_filenames:
        if filename.startswith('fact_') and filename.endswith('.py'):
            facts.append(filename.split('.')[0])
    return facts


def get_class(module_name, dimension=False, package=None):
    """
    Effectively does this:
    from fact/fact_count_all_the_sales import FactCountAllTheSales
    return FactCountAllTheSales

    Example usage:
    get_class('fact_count_all_the_sales')

    If dimension is True then it searches for a dimension instead.

    """
    module_name_parts = []
    if package:
        module_name_parts.append(package)
    module_name_parts.append("dim" if dimension else "fact")
    module_name_parts.append(module_name)
    qualified_module_name = ".".join(module_name_parts)

    module = importlib.import_module(qualified_module_name)
    class_name = underscore_to_camelcase(module_name)
    my_class = getattr(module, class_name)
    return my_class


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


def run_scripts(scripts):
    """
    Looks for the scripts in the folder 'scripts' at the root of the
    pylytics project, and runs them.

    """
    for script in scripts:
        log.info("Running setup script: %s", script)
        try:
            script = importlib.import_module('scripts.{}'.format(script))
        except ImportError as exception:
            log.error('Unable to import the script `{}`. '
                      'Traceback - {}.'.format(script, str(exception)))
            continue

        log.info('Running {}'.format(script))

        try:
            script.main()
        except Exception as error:
            log.error("Error occurred while running script: %s", error)


def find_scripts(command, fact_classes, script_type):
    """
    Introspects the list of fact_classes and returns a list of script names
    that need to be run.

    If the same script is listed in multiple fact classes, then it will
    only appear once in the list.

    """
    scripts = set()
    for fact_class in fact_classes:
        try:
            script_dict = getattr(fact_class, script_type)
        except AttributeError:
            log.warning('Unable to find {} in {}.'.format(
                        script_type, fact_class.__class__.__name__))
        else:
            if isinstance(script_dict, dict) and command in script_dict:
                scripts.update(script_dict[command])
    return scripts


class Commander(object):

    def __init__(self, db_name):
        self.db_name = db_name

    def run(self, command, *facts):
        """ Run command for each fact in facts.
        """
        from connection import DB
        errors = {}

        # Normalise the collection of facts supplied to remove duplicates,
        # expand "all" and report unknown facts.
        if "all" in facts:
            facts = all_facts()
        else:
            facts = set(facts)
            unknown_facts = facts - set(all_facts())
            for fact in unknown_facts:
                log.error("%s | Unknown fact", fact)
                facts.remove(fact)

        with DB(self.db_name) as database_connection:

            # Get all the fact classes.
            fact_classes = []
            for fact in facts:
                try:
                    FactClass = get_class(fact)(connection=database_connection)
                except Exception as error:
                    # Inline import as we're not making log object global.
                    log.error("Unable to load fact '{}' due to {}: {}"
                              .format(fact, error.__class__.__name__, error))
                else:
                    fact_classes.append(FactClass)

            # Execute any setup scripts that need to be run.
            setup_scripts = find_scripts(command, fact_classes, "setup_scripts")
            if setup_scripts:
                log.debug("Running setup scripts")
                run_scripts(setup_scripts)
            else:
                log.debug('No setup scripts to run')

            # Execute the command on each fact class.
            for fact_class in fact_classes:
                extra = {'table': fact_class.table_name}
                fact_class_name = fact_class.__class__.__name__
                log.debug("Calling {0}.{1}".format(fact_class_name, command),
                          extra=extra)
                try:
                    command_function = getattr(fact_class, command)
                except AttributeError:
                    log.error("Cannot find command %s for fact class %s",
                              command, fact_class, extra=extra)
                else:
                    command_function()

            # Execute any exit scripts that need to be run.
            exit_scripts = find_scripts(command, fact_classes, "exit_scripts")
            if exit_scripts:
                log.debug("Running exit scripts")
                run_scripts(exit_scripts)
            else:
                log.debug('No exit scripts to run')

        print_summary(errors)


def main():
    """ Main function called by the manage.py from the project directory.
    """
    from fact import Fact

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
        #choices = Fact.public_methods(),
        help = 'The command you want to run.',
        nargs = 1,
        type = str,
        )
    parser.add_argument(
        'fact',
        #choices = ['all'] + all_facts(),
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

    from pylytics.library.settings import Settings, settings

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
    if command in Fact.public_methods():
        commander.run(command, *args['fact'])
    else:
        log.error("Unknown command: %s", command)

    sys.stdout.write(bright_white("\nCompleted at {}\n\n".format(
        datetime.now())))
