import datetime
import warnings

from connection import DB
from table import Table
from main import get_class


class Fact(Table):
    """
    Fact base class.
    
    """
    dim_names = []
    dim_fields = []
    dim_modules = []
    dim_classes = []
    dim_map = {}
    historical_iterations = 100
    
    def __init__(self, *args, **kwargs):
        self.dim_or_fact = 'fact'
        super(Fact, self).__init__(*args, **kwargs)

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
        result = []

        for value in src_tuple:
            if type(value) == datetime.datetime:
                result.append(value.date())
            else:
                result.append(value)

        return tuple(result)

    def _import_dimensions(self):
        """
        Sets self.dim_map to a dictionary of dictionaries - each of them
        gives the mapping for all the dimensions linked to the fact.
        Sets self.dim_classes to a list of classes - one for each dimension.
        
        Example usage :
        > _import_dimensions()
        Example of self.dim_map :
        > {
        >     'location' : {
        >         'LON':1,
        >         'NY':2,
        >     },
        >     'thingtocount' : {
        >         '123':1,
        >         'ABC88':2,
        >         'XXX11':3,
        >         ...
        >     }
        >     ...
        >  }

        Example of self.dim_classes:
        > [pointer to location class, pointer to home class]
        > self.dim_classes[1].get_dictionnary('short_code')

        """
        # First thing to understand is why dim_names and dim_fields are 
        # separate...
        for dim_name, dim_field in zip(self.dim_names, self.dim_fields):
            
            if dim_field == None:
                self.dim_classes.append(None)
                self.dim_map[dim_name] = None
            else:
                # Import the modules and the class of the corresponding
                # dimensions.
                dim_class = get_class(dim_name, dimension=True)(
                                                    connection=self.connection)
                self.dim_classes.append(dim_class)
                    
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

        if self.dim_map == {}:
            self._import_dimensions()
        
        for (value, dim_name) in zip(src_tuple, self.dim_names):
            if self.dim_map[dim_name] == None:
                result.append(value)
                error = False
            else:
                try:
                    result.append(self.dim_map[dim_name][value])
                    error = False
                except:
                    result.append('NULL')
                    error = True

        return (tuple(result), error)
    
    def build(self):
        """
        Build and populate the dimensions required.
        
        """
        for dim_name in self.dim_names:
            if dim_name:
                dimension = get_class(dim_name, dimension=True)(
                                                    connection=self.connection)
                dimension.build()
                dimension.update()
        super(Fact, self).build()
    
    def update(self, historical=False, index=0):
        """
        Updates the fact table with the newest rows (modified since last
        update).
        """
        # Make sure all the tables have been created:
        self.build()
        
        # Status.
        msg = "Updating %s" % self.table_name
        self._print_status(msg)

        error_count = 0
        success_count = 0
        
        # Get the query.
        if not historical:
            query = self.source_query
        else:
            if not hasattr(self, 'historical_source_query'):
                warnings.warn('There is no historical_source_query defined!')
                return 0
            else:
                query = self.historical_source_query.format(index)
        
        # Get the full source list.
        data = []
        with DB(self.source_db) as database:
            data = database.execute(query)

        # Update the fact table with all the rows.
        for row in data:
            map_result = self._map_tuple(self._transform_tuple(row))
            destination_tuple = map_result[0]
            error = map_result[1]

            if error == False:
                try:
                    query = """
                        REPLACE INTO `%s` VALUES (NULL, %s, NULL)
                        """ % (self.table_name,
                               self._values_placeholder(
                                                    len(destination_tuple)))
                    self.connection.execute(query, destination_tuple)
                    success_count += 1
                except Exception as e:
                    print "--> MySQL error: %s" % str(destination_tuple)
                    print "- Row after _transform_tuple(): %s" % str(
                                                self._transform_tuple(row))
                    print "- Raw row from DB: %s" % str(row)
                    print(e)
                    error_count += 1
            else:
                print "--> Error on mapping: %s" % str(destination_tuple)
                print "- Row after _transform_tuple(): %s" % str(
                                                self._transform_tuple(row))
                print "- Raw row from DB: %s" % str(row)
                error_count += 1
        
        msg = "--> %s rows inserted, %s errors (i.e. rows not inserted)" % (
                                                    success_count, error_count)
        self._print_status(msg)
    
    def historical(self):
        """
        Run the historical_query - useful for rebuilding the tables from
        scratch.

        """
        for i in xrange(self.historical_iterations):
            if self.update(historical=True, index=i) == 0:
                break
