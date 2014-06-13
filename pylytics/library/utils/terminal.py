"""Provides a consistent interface for terminal output across the project."""

import datetime
import logging


log = logging.getLogger("pylytics")

MAP = {
    'red': '\033[31m',
    'blue': '\033[36m',
    'bold': '\033[1m',
    'green': '\033[32m',
    'reset': '\033[0m',
    'reverse': '\033[7m',
}


def format_text(text, format):
    if format not in MAP.keys():
        raise ValueError('Not a valid format option - pick:' \
                         ' {}'.format(', '.join(MAP.keys())))
    return MAP[format] + text + MAP['reset']


def print_status(message, timestamp=True, format=None, indent=True,
                 space=False):
    """
    Printing output with simple formatting options.
    """
    s = []
    if space:
        s.append("\n")
    
    if indent:
        s.append("  ")
    
    if timestamp:
        s.append('{}: '.format(datetime.datetime.now()))
    
    if format:
        s.append(format_text(message, format))
    else:
        s.append(message)

    log.info("".join(s))
