from mysql.connector.errors import OperationalError, ProgrammingError


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


class DatabaseGoneAwayError(OperationalError):
    """ Raised when the connection to a database is lost.

    See: https://dev.mysql.com/doc/refman/5.5/en/error-messages-client.html#error_cr_server_gone_error

    """
    code = 2006


class BrokenPipeError(OperationalError):
    """ Raised when the connection dies, usually from a stale connection being
    used.
    """
    code = 2055


class NoSuchTableError(ProgrammingError):
    """ Raised when a reference is made to a non-existent table.

    See: https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html#error_er_no_such_table

    """
    code = 1146


class ExistingTriggerError(ProgrammingError):
    """ Raised when a user is trying to create a trigger when one already
    exists.
    """
    code = 1235


def classify_error(error):
    """ Alter the class of an error to something specific instead of the
    generic error raised. This enables errors to be caught more cleanly
    rather than having to inspect and re-raise.
    """
    if isinstance(error, OperationalError):
        for error_class in [CantCreateTableError, BadNullError, BadFieldError,
                            DatabaseGoneAwayError, BrokenPipeError]:
            if error.args[0] == error_class.code:
                error.__class__ = error_class

    if isinstance(error, ProgrammingError):
        for error_class in [NoSuchTableError, TableExistsError,
                            ExistingTriggerError]:
            if error.args[0] == error_class.code:
                error.__class__ = error_class
