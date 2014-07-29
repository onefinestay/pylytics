import inspect
import json

from MySQLdb import IntegrityError

from build_sql import SQLBuilder
from group_by import GroupBy
from join import TableBuilder
from main import get_class
from pylytics.library.exceptions import BadFieldError, NoSuchTableError
from table import Table, SourceData


def raw_name(name):
    if name.startswith("dim_"):
        name = name[4:]
    if name.startswith("fact_"):
        name = name[5:]
    return name


def column_name(table, column):
    name = "%s_%s" % (raw_name(table), column)
    if name == "date_date":
        name = "date"
    return name


class Fact(Table):
    """
    Fact base class.

    """

    DELETE_FROM_STAGING = """\
    DELETE FROM staging WHERE id in ({ids})
    """
    DROP_VIEW = """\
    DROP VIEW IF EXISTS `vw_{table}`
    """
    REPLACE = """\
    REPLACE INTO `{table}` VALUES (NULL, {values}, NULL)
    """
    SELECT_FROM_STAGING = """\
    SELECT id, value_map FROM staging
    WHERE fact_table = '{table}'
    ORDER BY created, id
    """

    INTEGER = "INT(11)"

    # Load routine used for hydrating serialised data taken
    # from the staging table. This should be overridden if
    # required for more complex transformations, etc. This
    # function should take a string and return a dictionary.
    def load(self, s):
        return json.loads(s)

    dim_names = None
    metric_names = None

    historical_iterations = 100
    setup_scripts = {}
    exit_scripts = {}
    
    def __init__(self, *args, **kwargs):
        super(Fact, self).__init__(*args, **kwargs)
        self.dim_or_fact = 'fact'

        self.dim_classes = []
        self.dim_map = {}
        self.dim_modules = []
        self.types = self.types if hasattr(self, 'types') else None

        self.dim_dict = None
        if not hasattr(self, 'dim_links'):
            self.dim_links = self.dim_names

        self.dim_classes = [
            get_class(dim_link, dimension=True,
                      package=self.base_package)(connection=self.connection,
                                                 base_package=self.base_package)
            for dim_link in self.dim_links
        ]
        self.input_cols_names = self._get_cols_from_sql()

    @property
    def rolling_view_name(self):
        return "%s_rolling_view" % self.table_stem

    @property
    def midnight_view_name(self):
        return "%s_midnight_view" % self.table_stem

    def _transform_tuple(self, src_tuple):
        """
        Overwrite if needed while extending the class.
        Given a tuple representing a row of the source table (queried with
        self.source_query), returns a tuple representing a row of the fact
        table to insert.

        NB: - This function should be implemented when extending the fact
              object.
            - The columns in the returned tuple must be in the same order as in
              the fact table.
            - The first field (auto_increment `id`) and the last field
              (`created` automatic timestamp) must be omitted in the result.

        Example usage for a fact table like (id, name, attrib, created):
        > _transform_tuple(('name_val_in', 'attrib_val_in', 'unused value'))
        Returns :
        > ('name_val_out', 'attrib_val_out')

        """
        return src_tuple

    def _import_dimensions(self):
        """
        Sets self.dim_map to a dictionary of dictionaries - each of them
        gives the mapping for all the dimensions linked to the fact.
        Sets self.dim_classes to a list of classes - one for each dimension.

        Example usage:
        > _import_dimensions()
        Example of self.dim_map :
        > {
        >     'location': {
        >         'LON': 1,
        >         'NY': 2,
        >     },
        >     'thingtocount': {
        >         '123': 1,
        >         'ABC88': 2,
        >         'XXX11': 3,
        >         ...
        >     }
        >     ...
        >  }

        Example of self.dim_classes:
        > [pointer to location class, pointer to home class]
        > self.dim_classes[1].get_dictionary('short_code')

        """
        zipped = zip(self.dim_names, self.dim_classes, self.dim_fields)
        for dim_name, dim_class, dim_field in zipped:
            # Get the dictionary for each dimension.
            self.dim_map[dim_name] = dim_class.get_dictionary(dim_field)

    def _map_tuple(self, src_tuple):
        """
        Given a tuple of values, returns a new tuple (and an error code),
        where each value has been replaced by its corresponding dimension id.

        Example usage:
        > _map_tuple(('1223', 'LON', 'Live'))
        Returns:
        > (235, 1, 4)

        """
        result = []
        error = False

        for i, value in enumerate(src_tuple):
            if i in self.dim_dict:
                dim_name = self.dim_dict[i]
                try:
                    dim_value = self.dim_map[dim_name][value]
                except KeyError:
                    result.append(None)
                    error = True
                else:
                    result.append(dim_value)
                    error = False
            else:
                result.append(value)
                error = False

        return tuple(result), error

    def _build_dimensions(self):
        """ Build (and update) the dimension tables related to this fact.
        """

        for dim_class in self.dim_classes:
            dim_class.build()
            dim_class.update()

    def _auto_build(self, data):
        """ Auto-generate the structure and build the table (used if no SQL
        file exists).
        """
        if data.column_types is None:
            self.log_warning("Cannot auto-build table as no column "
                             "definitions available")
            return

        self.log_debug("Building table from auto-generated DDL")
        column_types = dict(data.column_types)
        column_types.update({name: self.INTEGER for name in self.dim_names})
        sql = SQLBuilder(
            table_name=self.table_name,
            cols_names=data.column_names,
            cols_types=column_types,
            unique_key=self.dim_names,
            foreign_keys=zip(self.dim_names, self.dim_links)
        ).query
        self.connection.execute(sql)
        self.log_debug("Table successfully built")

    @property
    def fixed_columns(self):
        """ Tuple of column names that are fixed and not pulled from the
        data source.
        """
        return self.surrogate_key_column, "created"

    def _get_cols_from_sql(self):
        sql = self.SELECT_NONE.format(table=self.table_name)
        try:
            result = self.connection.execute(sql, get_cols=True)
        except NoSuchTableError:
            self.log_warning("Unable to ascertain columns for "
                             "non-existent table")
        else:
            columns = result[1]
            return [_ for _ in columns if _ not in self.fixed_columns]

    def _get_query(self, historical, index):
        if not historical:
            query = self.source_query
        else:
            if not hasattr(self, 'historical_source_query'):
                self.log_warning('No historical_source_query defined')
                return 0
            else:
                query = self.historical_source_query.format(index)
        return query
    
    def _generate_dim_dict(self, data):
        self.dim_dict = {i: column_name
                         for i, column_name in enumerate(data.column_names)
                         if column_name in self.dim_names}

    def _fetch_from_source(self, historical=False, index=0, **kwargs):
        """ Get, joins and group the source data for this table. The data is
        returned as a `SourceData` instance that contains `column_names`,
        `column_types` and `rows`.
        """
        self.log_debug("Fetching rows from source database")
        
        # Initializing the table builder
        tb = TableBuilder(
            main_db=self.source_db,
            main_query=self._get_query(historical, index),
            create_query=None,
            output_table=self.table_name,
            cols=self.input_cols_names,
            types=self.types,
            verbose=True,
            )

        # Getting main data
        tb.add_main_source()

        # Joining extra data if required
        if hasattr(self, 'extra_queries'):
            for (extra_query, query_dict) in self.extra_queries.items():
                tb.add_source(name=extra_query, **query_dict)
        tb.join()

        data = SourceData(column_types=tb.result_cols_types)
        
        # Grouping by if required
        try:
            group_by = self.group_by
        except AttributeError:
            data.column_names = tb.result_cols_names
            data.rows = tb.result
        else:
            gb = GroupBy(tb.result, group_by,
                         cols=tb.result_cols_names, dims=self.dim_names)
            data.column_names = gb.output_cols
            data.rows = gb.process()

        return data

    def _fetch_from_staging(self, delete=False):
        """ Fetch data from staging table and inflate it ready for insertion.
        Should return a `SourceData` instance or raise a RuntimeError if the
        staging table cannot be found.
        """
        with self.warehouse_connection as connection:
            self.log_debug("Fetching rows from staging table")

            sql = self.SELECT_FROM_STAGING.format(table=self.table_name)
            try:
                results = connection.execute(sql)
            except NoSuchTableError:
                self.log_error("No staging table available, "
                               "cannot fetch records")
                return None

            column_names = self.dim_names + self.metric_names
            rows = []
            recycling = []
            self.log_debug("Extracting data for columns %s",
                           ", ".join(column_names))
            for id_, value_map in results:
                try:
                    data = self.load(value_map)
                except Exception as error:
                    self.log_error("Broken record (%s: %s) -- %s",
                                   error.__class__.__name__, error, value_map)
                else:
                    row = [data.get(key) for key in column_names]
                    rows.append(row)
                finally:
                    recycling.append(id_)

            source_data = SourceData(column_names=column_names, rows=rows)

            # Remove the rows we've processed, if any.
            if delete and recycling:
                sql = self.DELETE_FROM_STAGING.format(
                    ids=",".join(map(str, recycling)))
                connection.execute(sql)

            return source_data

    def _insert(self, data):
        self.log_debug("Inserting %s rows", len(data))

        not_matching_count = 0
        error_count = 0
        success_count = 0

        self._import_dimensions()
        self._generate_dim_dict(data)

        for source_values in data.rows:
            num_values = len(source_values)
            fact_values = self._transform_tuple(source_values)
            identity_values, not_matching = self._map_tuple(fact_values)
            placeholder_values = self._values_placeholder(num_values)
            query = self.REPLACE.format(table=self.table_name,
                                        values=placeholder_values)
            try:
                self.connection.execute(query, identity_values)
            except Exception as error:
                self.log_error("Error occurred while inserting data: %s", error)
                self.log_debug("Source values   = %s", source_values)
                self.log_debug("Fact values     = %s", fact_values)
                self.log_debug("Identity values = %s", identity_values)
                self.log_debug("SQL query       = %s", query.strip())
                error_count += 1
            else:
                success_count += 1

            if not_matching:
                not_matching_count += 1

        self.connection.commit()

        self.log_debug("%s rows inserted", success_count)
        if not_matching_count:
            self.log_error("%s of the rows inserted did not match dimensions",
                           not_matching_count)
        if error_count:
            self.log_error("%s errors during insert", error_count)

    def build(self, sql=None):
        """ Ensure the table is built.
        """
        data = self._fetch()
        self._build_dimensions()
        if not Table.build(self, sql):
            self._auto_build(data)
        self.create_or_replace_views()
        self.log_info("Built fact")

    def update(self):
        """ Update the fact table with the newest rows, first building the
        table if it doesn't already exist.
        """
        data = self._fetch(staging_delete=True)
        if data is None:
            self.log_info("No fact data available")
        else:
            # Data will be `None` if no staging table exists.
            self._build_dimensions()
            if not Table.build(self):
                self._auto_build(data)
            self._insert(data)
            self.create_or_replace_views()
            self.log_info("Updated fact with %s records", len(data))

    def _create_or_replace_rolling_view(self):
        """ Build a base level view against the table that explodes all
        dimension data into one wider set of columns.
        """
        columns = ["`fact`.`id` AS fact_id"]
        clauses = ["CREATE OR REPLACE VIEW `{view}` AS",
                   "SELECT\n    {columns}",
                   "FROM `{source}` AS fact"]

        for i, dim_class in enumerate(self.dim_classes):
            dim_table = dim_class.table_name
            columns.extend(
                "`%s`.`%s` AS %s" % (
                    self.dim_names[i],
                    column,
                    column_name(self.dim_names[i], column)
                    ) for column in dim_class.column_names[1:-1]
                )

            clauses.append(
                "INNER JOIN `%s` AS `%s` ON `%s`.`id` = `fact`.`%s`" % (
                    dim_table,
                    self.dim_names[i],
                    self.dim_names[i],
                    self.dim_names[i]
                    )
                )

        columns.extend(
            "`fact`.`%s` AS %s" % (
                d[0],
                raw_name(d[0])
                ) for d in self.description[1:-1]
                  if not d[0].startswith("dim_")
            )

        sql = "\n".join(clauses).format(
            view=self.rolling_view_name, source=self.table_name,
            columns=",\n    ".join(columns))

        self.connection.execute(sql)

    def _create_or_replace_midnight_view(self):
        """ Build a view atop the rolling view for this fact which holds
        everything up until last midnight. This view will only be built if
        a date dimension exists for this fact.
        """
        sql = """\
        CREATE OR REPLACE VIEW `{view}` AS
        SELECT * FROM `{source}`
        WHERE date(`date`) < CURRENT_DATE
        """.format(view=self.midnight_view_name, source=self.rolling_view_name)
        try:
            self.connection.execute(sql)
        except BadFieldError:
            # It's likely the `date` field isn't defined for this table.
            self.log_warning("Cannot create midnight view as no date "
                             "dimension exists for this fact")

    def create_or_replace_views(self):
        self._create_or_replace_rolling_view()
        self._create_or_replace_midnight_view()

    def historical(self):
        """
        Run the historical_query - useful for rebuilding the tables from
        scratch.

        """
        self.update()

        for i in xrange(1, self.historical_iterations):
            data = self._fetch_from_source(historical=True, index=i)
            if data:
                self._insert(data)
                self.log_info("Updated fact with %s records", len(data))

    @classmethod
    def public_methods(self):
        """
        Returns a list of all public method names on this class.
        """
        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        return [i[0] for i in methods if not i[0].startswith('_')]

    def drop(self, force=False):
        Table.drop(self, force)

        # Try to also drop the corresponding view if one exists.
        sql = self.DROP_VIEW.format(table=self.table_name)
        try:
            self.connection.execute(sql)
        except IntegrityError:
            self.log_error("Unable to drop view")
