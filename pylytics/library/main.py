import argparse
import datetime
import inspect
import logging
from logging.handlers import TimedRotatingFileHandler
import sys

import connection
from log import ColourFormatter, bright_white
from fact import Fact
from warehouse import Warehouse
from settings import Settings, settings


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
    facts_to_update = []

    for fact in all_fact_classes:
        if fact.__schedule__.should_run:
            facts_to_update.append(fact)

    return facts_to_update


class Commander(object):

    def run(self, command, *facts):
        """ Run command for each fact in facts.
        """
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

        if command != 'template':
            _connection = connection.get_named_connection(settings.pylytics_db)
            Warehouse.use(_connection)

        # Execute the command on each fact class.
        for fact_class in facts_to_run:
            try:
                command_function = getattr(fact_class, command)
            except AttributeError:
                log.error("Cannot find command %s for fact class %s",
                          command, fact_class)
                continue

            try:
                command_function()
            except Exception as exception:
                # Catch all exceptions so one failed command doesn't bring
                # down all facts.
                log.error("%s.%s failed: %s, %s", fact_class, command,
                    exception.__class__, exception.message)

        if command != 'template':
            # Close the Warehouse connection.
            log.info('Closing Warehouse connection.')
            Warehouse.get().close()


# TODO Make this configurable via settings.py.
def enable_logging():
    default_handler = logging.StreamHandler(sys.stdout)
    default_handler.setFormatter(ColourFormatter())
    log.addHandler(default_handler)

    error_handler = logging.handlers.TimedRotatingFileHandler(
        filename='/tmp/pylytics.log', when='D', backupCount=7)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    error_handler.setFormatter(formatter)
    error_handler.setLevel(logging.ERROR)
    log.addHandler(error_handler)

    log.setLevel(logging.DEBUG if __debug__ else logging.INFO)


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
        datetime.datetime.now())))

    # Enable log output before loading settings so we have visibility
    # of any load errors.
    enable_logging()

    # Prepend an extra settings file if one is specified.
    settings_module = args["settings"]
    if settings_module:
        settings.prepend(Settings.load(settings_module, from_path=True))

    # Attempt to configure Sentry logging.
    sentry_dsn = settings.SENTRY_DSN
    if sentry_dsn:
        # Only import raven if we're actually going to use it.
        from raven.handlers.logging import SentryHandler
        log.addHandler(SentryHandler(sentry_dsn))

    command = args['command'][0]
    commander = Commander()

    if command in ('update', 'historical'):
        commander.run('build', *args['fact'])
        commander.run(command, *args['fact'])
    elif command in ('build', 'template'):
        commander.run(command, *args['fact'])
    else:
        log.error("Unknown command: %s", command)

    sys.stdout.write(bright_white("\nCompleted at {}\n\n".format(
        datetime.datetime.now())))
