"""
This module allows you to get data from various SQL databases, "join" them
together within your Python script, and write the result into your own
database.

High-level usage:
    > from join import TableBuilder
    >
    > tb = TableBuilder(
    >   main_db = 'platform',
    >   main_query = "SELECT ...",
    >   output_table = 'Your_table',
    >   verbose = True
    > )
    > tb.quick_join(
    >   {
    >       'name': 'my_source1',
    >       'db': 'ecommerce',
    >       'query': 'SELECT id, ...',
    >       'join_on': 2
    >   },
    >   {
    >       'name': 'my_source2',
    >       'db': 'platform',
    >       'query': "SELECT id, ...",
    >       'join_on': 6,
    >       'outer_join':True
    >   }
    > )

Low-level usage:
    > from join import TableBuilder
    >
    > tb = TableBuilder(
    >   main_db = 'platform',
    >   main_query = "SELECT ...",
    >   output_table = 'Your_table',
    >   transform_row = (lambda row : row+['static_val1', None]),
    >   verbose = True
    > )
    > tb.build()
    > tb.add_source('my_source1', 'ecommerce', "SELECT id, ...", join_on=2
    > tb.add_source('my_source2', 'platform', "SELECT id, ...", join_on=6,
                    outer_join=True)
    > tb.join()
    > tb.write()
    > tb.reporting()

In this example, we will:
- download the main data using 'main_query',
- for each of those rows, join :
    - the first column of 'my_source1' on the 2nd column of 'main_query'
    - the first column of 'my_source2' on the 6nd column of 'main_query'
- write the result in 'Your_table' in this form:
    [cols from main data], [cols from 'my_source1', except the 1st one],
    [cols from 'my_source2', except the 1st one]

NB1: The 'transform_row' function is applied to each output row, just before
inserting them into the output table

NB2: The joined data is appended at the end of the main data, in the same order
you added the sources.

"""

import datetime

from connection import DB

###############################################################################

# Custom Exceptions

class SourceAlreadyExistsError(Exception):
    def __init__(self, value):
        self.value = value
    
    def __str__(self):
        return "Error while adding source '%s': it already exists" % self.value


class NoColumnToJoinError(Exception):
    def __init__(self, source_name):
        self.source_name = source_name
    
    def __str__(self):
        return """The source '%s' contains less than 2 columns. All the
        secondary sources should contain at least one column to join on,
        as well as one column to add.""" % self.source_name


class WrongColumnsNamesError(Exception):
    def __init__(self, not_matching_sql, not_matching_user_defined):
        self.not_matching_sql = not_matching_sql
        self.not_matching_user_defined = not_matching_user_defined
    
    def __str__(self):
        return """If you choose to specify the columns order, then the 'cols'
        list should contain exactly the same field names as the SELECT queries
        (you can also choose not to specify any 'cols' list : the alphabetical order
        will be used).
        
        The columns names %s you specified don't match the fields %s
        from your SELECT queries.""" % (list(self.not_matching_user_defined), list(self.not_matching_sql))

        
class UnknownColumnTypeError(Exception):
    def __init__(self, e):
        self.e = e
    
    def __str__(self):
        return """The type code %s has been retrieved from
        the initial SELECT queries but is not recognized by the
        'field_types' dictionnary.""" % self.e

###############################################################################

def get_dictionary(sequence):
    """
    Given a list of tuples, returns a dictionary where:
    - The key is the first element of each tuple.
    - The value is the tuple without its first value.
    
    Example:
        > get_dictionary([('a','b','c'),('x','y','z'),('u','v','w')]
        {'a':('b','c'), 'x':('y','z'), 'u':('v','w')}
    
    """
    dictionary = {}
    for element in sequence:
        dictionary[element[0]] = element[1:]
    return dictionary


class TableBuilder(object):
    """
    The main object you have to instantiate to join tables.
    
    """
    
    # List of SQL types
    field_types = {
         0: 'DECIMAL',
         1: 'INT(11)',
         2: 'INT(11)',
         3: 'INT(11)',
         4: 'FLOAT',
         5: 'DOUBLE',
         6: 'TEXT',
         7: 'TIMESTAMP',
         8: 'LONG',
         9: 'INT(11)',
         10: 'DATE',
         11: 'TIME',
         12: 'DATETIME',
         13: 'YEAR',
         14: 'DATE',
         15: 'VARCHAR(255)',
         16: 'BIT',
         246: 'DECIMAL',
         247: 'VARCHAR(255)',
         248: 'SET',
         249: 'TINYBLOB',
         250: 'MEDIUMBLOB',
         251: 'LONGBLOB',
         252: 'BLOB',
         253: 'VARCHAR(255)',
         254: 'VARCHAR(255)',
         255: 'VARCHAR(255)'
    }
    
    def __init__(self, main_db, main_query, output_table,
                 create_query=None, verbose=False, output_db=None,
                 cols=None, types=None, preliminary_query=None,
                 unique_key=None, foreign_keys=None,
                 transform_row=(lambda x: x)):
        self.sources = {}
        self.output_table = output_table
        self.create_query = create_query
        self.verbose = verbose
        self.result = []
        self._transform_row = transform_row
        self.output_db = output_db
        self.start_time = datetime.datetime.now()
        self.main_source = {
            'db': main_db,
            'query': main_query,
            'data': [],
            'cols_names': [],
            'cols_types': {}
        }
        self.result_cols_names = cols
        self.user_def_types = {} if types is None else types
        self.result_cols_types = None
        self.preliminary_query = preliminary_query
        self.unique_key = unique_key
        self.foreign_keys = foreign_keys
        self._get_data(None)
        
    def _print_status(self, message):
        """Use this for all printing all output."""
        if self.verbose:
            print message
    
    def _get_create_query(self):
        """
        Builds the CREATE query, based on the fields names
        and types.
        
        """
        query = 'CREATE TABLE %s (\n' % self.output_table
        
        query += '  `id` INT(11) NOT NULL AUTO_INCREMENT'        
        for col in self.result_cols_names:
            query += '  ,`%s` %s DEFAULT NULL' % (col, self.result_cols_types[col])
        query += '   ,`created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'
        query += '  ,PRIMARY KEY (`id`)'
        
        if self.unique_key is not None :
            query += '  ,UNIQUE KEY `%s_uk` (%s)' % (self.output_table, ','.join(['`'+e+'`' for e in self.unique_key]))
        
        if self.foreign_keys is not None :
            for i,(fk,ref) in enumerate(self.foreign_keys):
                query += '  ,CONSTRAINT `%s_ibfk_%s` FOREIGN KEY (`%s`) REFERENCES `%s` (`id`)' % (self.output_table, i, fk, ref)

        query += ') ENGINE=INNODB DEFAULT CHARSET=utf8'
        
        return query
    
    def _rebuild_sql(self):
        """
        Drops and rebuilds the output table.
        
        """
        self._print_status("(Re)-creating the output table.")

        if self.create_query:
            with DB(self.output_db) as database:
                query1 = "DROP TABLE IF EXISTS `" + self.output_table + "`"
                database.execute(query1)
                query2 = self.create_query
                database.execute(query2)
    
    def add_source(self, name, db, query, join_on, outer_join=False):
        """
        Adds a secondary source to the dictionary ('self.sources').
        
        - outer_join: If True, all the rows of the main source will be kept,
                      even if several of them doesn't match any row in this
                      source.
        
        - join_on:    The index of the column (in the main source) on which you
                      want to join this source (starting at 0). Thus, the
                      'join_on'-th column of the main source will be joined on
                      the first (index 0) column of this source.

        """
        if name in self.sources or name == 'main':
            raise SourceAlreadyExistsError(name)
        else:
            self.sources[name] = {
                'db': db,
                'query': query,
                'join_on': join_on,
                'outer_join': outer_join,
                'errors_count': 0,
                'matches_count': 0,
                'errors': [],
                'data': None,
                'cols_names': [],
                'cols_types': {}
            }
        
        self._get_data(name)
    
    def _get_data(self, source_name):
        """
        Gets the data from a source and stores it.
        
        'source_name' should be either the name provided in 'add_source()' or
        None to get data from the main source.
        
        """
        self._print_status("Getting data from '%s' source." % (
                                                        source_name or 'main'))

        if source_name == None:
            source = self.main_source
        else :
            source = self.sources[source_name]
            
        data = None 
        with DB(source['db']) as db:
            if self.preliminary_query != None:
                db.execute(self.preliminary_query)
            
            data = db.execute(source['query'], get_cols=True)
        
        # For the main source
        if source_name == None:
            source['data'] = data[0]
            source['cols_names'] = [e[0] for e in data[1]]
            try:
                source['cols_types'] = {e[0]:self.field_types[e[1]] for e in data[1]}
            except Exception as e:
                raise UnknownColumnTypeError(e)
        
        # For an extra source
        else :
            source['data'] = get_dictionary(data[0])
            if data[1] <= 1:
                raise NoColumnToJoinError(source_name)
            else:
                source['cols_names'] = [e[0] for e in data[1][1:]]
                try:
                    source['cols_types'] = {e[0]:self.field_types[e[1]] for e in data[1][1:]}
                except Exception as e:
                    raise UnknownColumnTypeError(e)
        
    def _get_cols_info(self):
        """
        Gathers all the 'cols_names' and 'cols_types' (one per
        source) into 'result_cols_names' and 'result_cols_types'.
        If the columns order was not specified through'result_cols_names',
        they are sorted alphabetically.
        
        """
        # Auto-retreiving columns types
        self.result_cols_types = self.main_source['cols_types']

        for s in self.sources.values():
            self.result_cols_types.update(s['cols_types'])
           
        # Overwriting it by user-defined types
        self.result_cols_types.update(self.user_def_types)
        
        # Retreiving columns names (if not user-defined)
        if self.result_cols_names is None:
            self.result_cols_names = sorted(self.result_cols_types.keys())
        
        # Checking if columns name are right (if user-defined)
        matching = set(self.result_cols_types.keys()) & set(self.result_cols_names)
        not_matching_sql = set(self.result_cols_types.keys()) - matching
        not_matching_user_defined = set(self.result_cols_names) - matching
        if len(not_matching_sql) + len(not_matching_user_defined) > 0:
            raise WrongColumnsNamesError(not_matching_sql, not_matching_user_defined)
            
    def _append_result_row(self, row, matches):
        """
        Given a row (of the main source) and the dictionary of the matches for
        each source, returns the final joined row.
        
        Example :
        > self.sources
        {
            'codes':{
                join_on: 1,
                ...
            }
            'people':{
                join_on: 3,
                ...
            }
        }
        > self._append_result_row((5,1258,'note',6,2013),
        {'codes':('ABCD','London'), 'people':('John',)})
        [5,'ABCD','London','note',6,'John',2013]
        
        """
        result_row_unordered = {}
        result_row = []
        checks = [matches[match][0] != None or self.sources[match]['outer_join'] for match in matches]
        
        if all(checks):
            # Adding all the fields from the main source
            result_row_unordered.update(zip(self.main_source['cols_names'], row))
            
            # Concatenating the joined fields
            for s_name,s in self.sources.items():
                result_row_unordered.update(zip(s['cols_names'], matches[s_name]))
            
            # Getting the values in the right order
            result_row = [result_row_unordered[f] for f in self.result_cols_names]
            
            self.result.append(self._transform_row(result_row))
    
    def join(self):
        """
        Joins all the secondary sources to the main source (stores the
        result in 'self.result', and the columns informations in
        'self.result_cols_names' and 'self.result_cols_types').
        
        """
        self._print_status("Joining sources.")
        
        self._get_cols_info()
        
        for row in self.main_source['data']:
            matches = {}
            
            for source_name, source_data in self.sources.items():
                join_on_index = source_data['join_on']
                join_on_value = row[join_on_index]
                try:
                    matching_row = source_data['data'][join_on_value]
                    matches[source_name] = matching_row
                except KeyError:
                    matches[source_name] = (None,) * len(source_data['cols_names'])
                    source_data['errors'].append(join_on_value)
                    source_data['errors_count'] += 1
                else:
                    source_data['matches_count'] += 1
            
            self._append_result_row(row, matches)
        
        if self.create_query is None:
            self.create_query = self._get_create_query()
    
    def _write_data_batches(self, query):
        """
        Executes the given insert query for each line of the 'self.result'
        data. The inserts are performed by batches of 1000.
        
        """
        N = 1000
        L = (len(self.result) + 1) / N + ((len(self.result) + 1) % N > 0)
        
        with DB(self.output_db) as dw:
            for i in range(L):
                dw.execute(query, values = self.result[i * N : (i + 1) * N],
                           many = True)
    
    def write(self, rebuild=True):
        """
        Writes the content of 'self.result' into the SQL output table.
        
        """
        if rebuild:
            self._rebuild_sql()
        
        self._print_status("Writing the data into the datawarehouse.")
        
        if len(self.result) > 0:
            with DB(self.output_db) as dw:
                query = "INSERT INTO {0} ({1}) VALUES ({2})".format(
                    self.output_table,
                    ",".join(self.result_cols_names),
                    ",".join(["%s"] * len(self.result[0]))
                    )                
                self._write_data_batches(query)
        
    def reporting(self):
        """
        For reporting purposes - shows the results of the script
        
        - 'rows':      The total number of rows in the given source.
        - 'matches':   The number of rows of the main source that match a row
                       in the given source
        - 'errors':    The number of rows of the main source that doesn't match
                       any row in the given source
                       In the verbose mode, displays the list of the keys (from
                       the main source) that doesn't match.

        """
        print "- Main source (from '%s'): %s rows" % (
            self.main_source['db'], len(self.main_source['data']))
        
        for s_name, s in self.sources.items():
            print "- Source '%s': %s rows %s matches %s errors" % (s_name,
                len(s['data']), s['matches_count'], s['errors_count'])
            if s['errors_count'] > 0:
                self._print_status(
                    "* Keys not found: " + ("   ".join(map(
                        lambda x: '"' + str(x) + '"', s['errors']))
                        )
                )
        
        print "(Execution started at: %s)" % self.start_time
        print "(Execution time: %s)" % (datetime.datetime.now() -
                                        self.start_time)
    
    def quick_join(self, **extra_queries):
        """
        High-level function to use the library (takes a list of sources):
        gets the data, performs the join and writes the output.
        
        Example:
            > self.quick_join(
                [{
                    'name': '...',
                    'db': '...',
                    'query': '...',
                    'join_on': '...'
                },
                {
                    'name': '...',
                    'db': '...',
                    'query': '...',
                    'join_on': '...',
                    'outer_join': True
                }]
            )
        
        """
        for query_name, extra_query in extra_queries.items():
            self.add_source(name=query_name, **extra_query)
        
        self.join()
        self.write(rebuild=True)
        self.reporting()
        