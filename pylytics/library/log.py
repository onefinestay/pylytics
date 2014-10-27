from __future__ import unicode_literals
from datetime import datetime
import logging


def natural(s):
    return s


def red(s):
    return "\x1b[31m{}\x1b[0m".format(s)


def green(s):
    return "\x1b[32m{}\x1b[0m".format(s)


def yellow(s):
    return "\x1b[33m{}\x1b[0m".format(s)


def blue(s):
    return "\x1b[34m{}\x1b[0m".format(s)


def magenta(s):
    return "\x1b[35m{}\x1b[0m".format(s)


def cyan(s):
    return "\x1b[36m{}\x1b[0m".format(s)


def white(s):
    return "\x1b[37m{}\x1b[0m".format(s)


def bright_black(s):
    return "\x1b[30;1m{}\x1b[0m".format(s)


def bright_red(s):
    return "\x1b[31;1m{}\x1b[0m".format(s)


def bright_yellow(s):
    return "\x1b[33;1m{}\x1b[0m".format(s)


def bright_blue(s):
    return "\x1b[34;1m{}\x1b[0m".format(s)


def bright_cyan(s):
    return "\x1b[36;1m{}\x1b[0m".format(s)


def bright_white(s):
    return "\x1b[37;1m{}\x1b[0m".format(s)


time_colour = bright_black
message_colours = {
    logging.DEBUG: white,
    logging.INFO: white,
    logging.WARNING: yellow,
    logging.ERROR: bright_yellow,
    logging.CRITICAL: bright_red,
}
highlight_colours = {
    logging.DEBUG: cyan,
    logging.INFO: cyan,
    logging.WARNING: cyan,
    logging.ERROR: bright_cyan,
    logging.CRITICAL: bright_cyan,
}


class ColourFormatter(logging.Formatter):

    def format(self, record):
        message = super(ColourFormatter, self).format(record)
        try:
            message_colour = message_colours[record.levelno]
            highlight_colour = highlight_colours[record.levelno]
        except KeyError:
            message_colour = natural
            highlight_colour = bright_white
        time = time_colour(datetime.now().time())
        try:
            table = "[" + record.table + "]"
        except AttributeError:
            return "%s  %s" % (time, message_colour(message))
        else:
            return "%s  %s %s" % (
                time, highlight_colour(table), message_colour(message))
