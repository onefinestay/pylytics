from datetime import datetime
import logging


def yellow(s):
    return "\x1b[33m{}\x1b[0m".format(s)


def blue(s):
    return "\x1b[34m{}\x1b[0m".format(s)


def cyan(s):
    return "\x1b[36m{}\x1b[0m".format(s)


def white(s):
    return "\x1b[37m{}\x1b[0m".format(s)


def bright_black(s):
    return "\x1b[30;1m{}\x1b[0m".format(s)


def bright_yellow(s):
    return "\x1b[33;1m{}\x1b[0m".format(s)


def bright_red(s):
    return "\x1b[31;1m{}\x1b[0m".format(s)


def bright_white(s):
    return "\x1b[37;1m{}\x1b[0m".format(s)


colours = {
    logging.DEBUG: blue,
    logging.INFO: white,
    logging.WARNING: yellow,
    logging.ERROR: bright_yellow,
    logging.CRITICAL: bright_red,
}


class ColourFormatter(logging.Formatter):

    def format(self, record):
        s = super(ColourFormatter, self).format(record)
        try:
            colour = colours[record.levelno]
        except KeyError:
            pass
        else:
            s = colour(s)
        time = bright_black(datetime.now().time())
        return "{}  {}".format(time, s)
