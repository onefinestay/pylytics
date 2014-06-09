import inspect
import json
import warnings

from build_sql import SQLBuilder
from group_by import GroupBy
from join import TableBuilder
from main import get_class
from pylytics.library.exceptions import NoSuchTableError
from table import Table, SourceData


class Fact(Table):
    """
    Fact base class.
    
    """

    DELETE_FROM_STAGING = "DELETE FROM staging WHERE id in ({ids})"
    INTEGER = "INT(11)"
    REPLACE = "REPLACE INTO `{table}` VALUES (NULL, {values}, NULL)"
    SELECT_FROM_STAGING = """\
    SELECT id, value_map FROM staging
    WHERE fact_table = '{table}'
    ORDER BY created, id
    """
    SELECT_NONE = "SELECT * FROM `{table}` LIMIT 0,0"

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
            get_class(dim_link, dimension=True, package=self.base_package)(connection=self.connection, base_package=self.base_package)
            for dim_link in self.dim_links
        ]
        self.input_cols_names = self._get_cols_from_sql()
        
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
            self._print_status("Cannot auto-build table: "
                               "No column definitions available")
            return

        self._print_status("Building table from auto-generated DDL")
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
        self._print_status("Table successfully built")

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
            self._print_status("Unable to ascertain columns for "
                               "non-existent table {}".format(self.table_name))
        else:
            columns = result[1]
            return [_ for _ in columns if _ not in self.fixed_columns]

    def _get_query(self, historical, index):
        if not historical:
            query = self.source_query
        else:
            if not hasattr(self, 'historical_source_query'):
                warnings.warn('There is no historical_source_query defined!')
                return 0
            else:
                query = self.historical_source_query.format(index)
        return query
    
    def _generate_dim_dict(self, data):
        self.dim_dict = {i: column_name
                         for i, column_name in enumerate(data.column_names)
                         if column_name in self.dim_names}

    def _fetch_from_source(self, historical=False, index=0):
        """ Get, joins and group the source data for this table. The data is
        returned as a `SourceData` instance that contains `column_names`,
        `column_types` and `rows`.
        """
        self._print_status("Fetching data from source "
                           "database for {}".format(self.table_name))
        
        # Initializing the table builder
        tb = TableBuilder(
            main_db=self.source_db,
            main_query=self._get_query(historical, index),
            create_query=None,
            output_table=self.table_name,
            cols=self.input_cols_names,
            types=self.types,
            verbose=True
            )
        
        # Getting main data
        tb.add_main_source()
        
        # Joining extra data if required
        if hasattr(self, 'extra_queries') :
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

    def _fetch_from_staging(self):
        """ Fetch data from staging table and inflate it ready for insertion.
        Should return a `SourceData` instance or raise a RuntimeError if the
        staging table cannot be found.
        """
        sql = self.SELECT_FROM_STAGING.format(table=self.table_name)
        results = self.connection.execute(sql)

        column_names = self.dim_names + self.metric_names
        rows = []
        recycling = []
        for id_, value_map in results:
            data = json.loads(value_map)
            row = [data[key] for key in column_names]
            rows.append(row)
            recycling.append(id_)

        source_data = SourceData(column_names=column_names, rows=rows)

        # Remove the rows we've processed, if any.
        if recycling:
            sql = self.DELETE_FROM_STAGING.format(
                ids=",".join(map(str, recycling)))
            self.connection.execute(sql)

        return source_data

    def _insert(self, data):
        self._print_status("Inserting into {}".format(self.table_name))

        not_matching_count = 0
        error_count = 0
        success_count = 0

        self._import_dimensions()
        self._generate_dim_dict(data)

        for row in data.rows:

            destination_tuple, not_matching = self._map_tuple(
                self._transform_tuple(row))

            values = self._values_placeholder(len(destination_tuple))
            query = self.REPLACE.format(table=self.table_name, values=values)
            try:
                self.connection.execute(query, destination_tuple)
            except Exception as e:
                self._print_status("MySQL error: {}".format(destination_tuple))
                self._print_status("Row after _transform_tuple(): %s" % (
                                        str(self._transform_tuple(row))))
                self._print_status("Raw row from DB: %s" % str(row))
                self._print_status(repr(e))
                error_count += 1
            else:
                success_count += 1

            if not_matching:
                not_matching_count += 1

        self.connection.commit()

        msg = "{0} rows inserted, {1} of which don't match the dimensions. " \
              "{2} errors happened.".format(success_count, not_matching_count,
                                            error_count)
        self._print_status(msg, format='green')
        
    def build(self, sql=None):
        """ Ensure the table is built.
        """
        data = self._fetch()
        self._build_dimensions()
        if not Table.build(self, sql):
            self._auto_build(data)

    def update(self):
        """ Update the fact table with the newest rows, first building the
        table if it doesn't already exist.
        """
        data = self._fetch()
        self._build_dimensions()
        if not Table.build(self):
            self._auto_build(data)
        self._insert(data)

    def historical(self):
        """
        Run the historical_query - useful for rebuilding the tables from
        scratch.

        """
        self.update()

        for i in xrange(1, self.historical_iterations):
            data = self._fetch_from_source(historical=True, index=i)
            self._insert(data)

    @classmethod
    def public_methods(self):
        """
        Returns a list of all public method names on this class.
        """
        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        return [i[0] for i in methods if not i[0].startswith('_')]
