import datetime
import inspect
import warnings

from build_sql import SQLBuilder
from connection import DB
from group_by import GroupBy
from join import TableBuilder
from main import get_class
from table import Table


class Fact(Table):
    """
    Fact base class.
    
    """
    
    historical_iterations = 100
    setup_scripts = {}
    exit_scripts = {}
    
    def __init__(self, *args, **kwargs):
        self.dim_or_fact = 'fact'
        
        self.dim_classes = []
        self.dim_map = {}
        self.dim_modules = []
        self.types = self.types if hasattr(self, 'types') else None

        self.output_data = None
        self.output_cols_names = None
        self.output_cols_types = None
        self.dim_dict = None
        if not hasattr(self, 'dim_links'):
            self.dim_links = self.dim_names

        super(Fact, self).__init__(*args, **kwargs)
        
        self.dim_classes = [get_class(dim_link, dimension=True)(connection=self.connection) for dim_link in self.dim_links]
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
        """
        result = []
        for value in src_tuple:
            if type(value) == datetime.datetime:
                result.append(value.date())
            else:
                result.append(value)
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
        for dim_name, dim_class, dim_field in zip(self.dim_names, self.dim_classes, self.dim_fields):
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
        
        for i,value in enumerate(src_tuple):
            if i in self.dim_dict:
                dim_name = self.dim_dict[i]
                try:
                    result.append(self.dim_map[dim_name][value])
                    error = False
                except:
                    result.append(None)
                    error = True
            else:
                result.append(value)
                error = False

        return (tuple(result), error)

    def _build(self):
        """
        Build and populate the dimensions required, and build the fact table
        (from .sql file if exists, from auto-generated structure if not).
        
        """
        for dim_class in self.dim_classes:
            dim_class.build()
            dim_class.update()
           
        table_built = super(Fact, self).build()
        
        if not table_built:
            # If the .sql file doesn't exist, auto-generate the structure and
            # build the table.
            self.output_cols_types.update({d:'INT(11)' for d in self.dim_names})
            
            sql = SQLBuilder(
                table_name=self.table_name,
                cols_names=self.output_cols_names,
                cols_types=self.output_cols_types,
                unique_key=self.dim_names, 
                foreign_keys=zip(self.dim_names,self.dim_links)
                ).query
            self.connection.execute(sql)
            
            self._print_status('Table built.')
            
    def _get_cols_from_sql(self):
        try:
            cols_names = self.connection.execute("SELECT * FROM `%s` LIMIT 0,0" % self.table_name,
                                           get_cols=True)[1]
            return filter(lambda x : x not in ['id','created'], cols_names)
        except Exception, e:
            if 1146 not in e.args:
                # If an error other than "table doesn't exists" happens
                raise

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
    
    def _generate_dim_dict(self):
        self.dim_dict = {i:d for i,d in enumerate(self.output_cols_names) if d in self.dim_names}
    
    def _process_data(self, historical=False, index=0):
        """
        Gets, joins and groups the data.
        Outputs the result into 'self.output_cols_names',
        'self.output_cols_types' and 'self.output_data'
        
        """
        # Status.
        self._print_status("Updating {}".format(self.table_name))
        
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
        self.output_cols_types = tb.result_cols_types
        
        # Grouping by if required
        if hasattr(self, 'group_by'):
            group_by = self.group_by
            gb = GroupBy(tb.result, group_by, cols=tb.result_cols_names, dims=self.dim_names)
            self.output_cols_names = gb.output_cols
            self.output_data = gb.process()
        else:
            self.output_cols_names = tb.result_cols_names
            self.output_data = tb.result
           
    def _insert_rows(self):
        not_matching_count = 0
        error_count = 0
        success_count = 0
        
        self._import_dimensions()
        self._generate_dim_dict()

        for row in self.output_data:
            destination_tuple, not_matching = self._map_tuple(self._transform_tuple(row))

            try:
                query = """
                    REPLACE INTO `%s` VALUES (NULL, %s, NULL)
                    """ % (self.table_name,
                           self._values_placeholder(
                                                len(destination_tuple)))
                self.connection.execute(query, destination_tuple)
                success_count += 1
            except Exception, e:
                self._print_status("MySQL error: %s" % str(
                                                        destination_tuple))
                self._print_status("Row after _transform_tuple(): %s" % (
                                        str(self._transform_tuple(row))))
                self._print_status("Raw row from DB: %s" % str(row))
                self._print_status(repr(e))
                error_count += 1
                
            if not_matching:
                not_matching_count += 1
        
        msg = "{0} rows inserted, {1} of which don't match the dimensions. " \
              "{2} errors happened.".format(success_count, not_matching_count,
                                            error_count)
        self._print_status(msg, format='green')
        
    def build(self):
        """
        Build only (without updating)
        
        """
        self._process_data()
        self._build()
        
    def update(self):
        """
        Updates the fact table with the newest rows.
        
        """
        self._process_data(historical=False, index=0)
        self._build() 
        self._insert_rows()

    def historical(self):
        """
        Run the historical_query - useful for rebuilding the tables from
        scratch.

        """
        self.update()

        for i in xrange(1, self.historical_iterations):
            self._process_data(historical=True, index=i)
            self._insert_rows()

    @classmethod
    def public_methods(self):
        """
        Returns a list of all public method names on this class.
        """
        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        return [i[0] for i in methods if not i[0].startswith('_')]
