from MySQLdb import OperationalError, ProgrammingError


class CantCreateTableError(OperationalError):
    """ Raised when a table cannot be created.

    https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html#error_er_cant_create_table

    """
    code = 1005


class BadNullError(OperationalError):
    """ Raised when an attempt is made to insert a NULL where it cannot go.

    https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html#error_er_bad_null_error

    """
    code = 1048


class TableExistsError(OperationalError):
    """ Raised when an attempt is made to create a table that already exists.

    https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html#error_er_table_exists_error

    """
    code = 1050


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


# TODO: tidy up this function
def classify_error(error):
    """ Alter the class of an error to something specific instead of the
    generic error raised. This enables errors to be caught more cleanly
    rather than having to inspect and re-raise.
    """
    if isinstance(error, OperationalError):
        if error.args[0] == CantCreateTableError.code:
            error.__class__ = CantCreateTableError
        elif error.args[0] == BadNullError.code:
            error.__class__ = BadNullError
        elif error.args[0] == TableExistsError.code:
            error.__class__ = TableExistsError
        elif error.args[0] == BadFieldError.code:
            error.__class__ = BadFieldError
    if isinstance(error, ProgrammingError):
        if error.args[0] == NoSuchTableError.code:
            error.__class__ = NoSuchTableError
