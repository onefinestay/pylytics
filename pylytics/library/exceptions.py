from MySQLdb import OperationalError, ProgrammingError


class BadFieldError(OperationalError):
    code = 1054


class NoSuchTableError(ProgrammingError):
    code = 1146


def classify_error(error):
    """ Alter the class of an error to something specific instead of the
    generic error raised. This enables errors to be caught more cleanly
    rather than having to inspect and re-raise.
    """
    if isinstance(error, OperationalError):
        if error.args[0] == BadFieldError.code:
            error.__class__ = BadFieldError
    if isinstance(error, ProgrammingError):
        if error.args[0] == NoSuchTableError.code:
            error.__class__ = NoSuchTableError
