import json
import logging

# TODO Can replace this with SQLAlchemy?
from pylytics.library.connection import DB
from table import Table


__all__ = ['Source', 'DatabaseSource', 'Staging']
log = logging.getLogger("pylytics")


class Source(object):
    """ Base class for data sources used by `fetch`.
    """

    @classmethod
    def define(cls, **attributes):
        return type(cls.__name__, (cls,), attributes)

    @classmethod
    def select(cls, for_class, since=None):
        """ Select data from this data source and yield each record as an
        instance of the fact class provided.
        """
        raise NotImplementedError("No select method defined for this source")

    @classmethod
    def finish(cls, for_class):
        """ Mark a selection as finished, performing any necessary clean-up
        such as deleting rows. By default, this method takes no action but
        can be overridden by subclasses.
        """
        pass


class DatabaseSource(Source):
    """ Base class for remote databases used as data sources for table
    data. This class is intended to be overridden with the `database`
    and `query` attributes populated with appropriate values.

    (See unit tests for example of usage)

    """

    @classmethod
    def execute(cls, **params):
        database = getattr(cls, "database")
        query = getattr(cls, "query").format(
            **{key: dump(value) for key, value in params.items()})
        with DB(database) as connection:
            rows, col_names, _ = connection.execute(query, get_cols=True)
        for row in rows:
            yield zip(col_names, row)

    @classmethod
    def select(cls, for_class, since=None):
        for record in cls.execute(since=since):
            yield hydrated(for_class, record)


class Staging(Source, Table):
    """ Staging is both a table and a data source.
    """
    __tablename__ = "staging"

    # Keep details of records to be deleted.
    __recycling = set()

    id = PrimaryKey()
    event_name = Column("event_name", unicode, size=80)
    value_map = Column("value_map", unicode, size=2048)
    created = CreatedTimestamp()

    @classmethod
    def _apply_expansions(cls, data):
        try:
            expansions = getattr(cls, "expansions")
        except AttributeError:
            pass
        else:
            for exp in expansions:
                if isinstance(exp, type) and issubclass(exp, DatabaseSource):
                    for record in exp.execute(**data):
                        data.update(record)
                elif hasattr(exp, "__call__"):
                    exp(data)
                else:
                    log.debug("Unexpected expansion type: %s",
                              exp.__class__.__name__)

    @classmethod
    def select(cls, for_class, since=None):
        extra = {"table": for_class.__tablename__}

        connection = Warehouse.get()
        log.debug("Fetching rows from staging table", extra=extra)

        events = list(getattr(cls, "events"))
        sql = """\
        SELECT id, event_name, value_map FROM staging
        WHERE event_name IN (%s)
        ORDER BY created, id
        """ % ",".join(map(dump, events))
        log.debug(sql)
        results = connection.execute(sql)

        for id_, event_name, value_map in results:
            try:
                data = {"__event__": event_name}
                data.update(json.loads(value_map))
                cls._apply_expansions(data)
                inst = hydrated(for_class, data)
            except Exception as error:
                log.error("Unable to hydrate %s record (%s: %s) -- %s",
                          for_class.__name__, error.__class__.__name__, error,
                          value_map, extra=extra)
            else:
                yield inst
            finally:
                # We'll recycle the row regardless of whether or
                # not we've been able to hydrate and yield it. If
                # broken, it gets logged anyway.
                cls.__recycling.add(id_)

    @classmethod
    def finish(cls, for_class):
        if cls.__recycling:
            sql = "DELETE FROM staging WHERE id in (%s)" % (
                ",".join(map(str, cls.__recycling)))
            Warehouse.execute(sql)
            cls.__recycling.clear()

    def __init__(self, event_name, value_map):
        self.event_name = event_name
        self.value_map = json.dumps(value_map, separators=",:")
