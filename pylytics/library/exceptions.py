from MySQLdb import OperationalError, ProgrammingError


class BadFieldError(OperationalError):
    """ Raised when a reference is made to an unknown column.

    See: https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html#error_er_bad_field_error

    """
    code = 1054


class NoSuchTableError(ProgrammingError):
    """ Raised when a reference is made to a non-existent table.

    See: https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html#error_er_no_such_table

    """
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
