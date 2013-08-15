"""Provides a consistent interface for terminal output across the project."""

from contextlib import contextmanager
import datetime
import sys


MAP = {
    'red': '\033[31m',
    'blue': '\033[36m',
    'bold': '\033[1m',
    'green': '\033[32m',
    'reset': '\033[0m',
    'reverse': '\033[7m',
}


@contextmanager
def format_text(format):
    if format not in MAP.keys():
        raise ValueError('Not a valid format option - pick:' \
                         ' {}'.format(', '.join(MAP.keys())))
    sys.stdout.write(MAP[format])
    yield
    sys.stdout.write(MAP['reset'])


def print_status(message, timestamp=True, format=None, indent=True,
                 space=False):
    """
    Printing output with simple formatting options.
    """
    if space:
        sys.stdout.write('\n')
    
    if indent:
        sys.stdout.write('   ')
    
    if timestamp:
        sys.stdout.write('{}: '.format(datetime.datetime.now()))
    
    if format:
        with format_text(format):
            sys.stdout.write(message)
    else:
        sys.stdout.write(message)
    
    sys.stdout.write('\n')
