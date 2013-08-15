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
from build_sql import SQLBuilder
from utils.terminal import print_status

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
        return "The source '{}' contains less than 2 columns. All the " \
               "secondary sources should contain at least one column to " \
               "join on, as well as one column to add.\r".format(
                                                            self.source_name)


class WrongColumnsNamesError(Exception):
    def __init__(self, not_matching, sql_fields):
        self.not_matching = not_matching
        self.sql_fields = sql_fields
    
    def __str__(self):
        return "The column names {0} don't match any of the fields {1} from " \
               "your SELECT queries\n".format(
                            list(self.not_matching), list(self.sql_fields))


class UnknownJoinOnColumnError(Exception):
    def __init__(self, col, source):
        self.col = col
        self.source = source
    
    def __str__(self):
        return "The column '{0}' from source '{1}' you are trying to join " \
               "on is unknown in the SQL field list.\n".format(self.col,
                                                                self.source)

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
    def __init__(self, main_db, main_query, output_table,
                 create_query=None, verbose=False, output_db=None,
                 cols=None, types=None, preliminary_query=None,
                 unique_key=None, foreign_keys=None, keys=None,
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
        self.keys = keys

    def _print_status(self, message, **kwargs):
        print_status(message, **kwargs)
    
    def _rebuild_sql(self):
        """
        Drops and rebuilds the output table.
        
        """
        self._print_status("(Re)-creating the output table.")
        
        if self.create_query is None:
            self.create_query = SQLBuilder(
                table_name=self.output_table,
                cols_names=self.result_cols_names,
                cols_types=self.result_cols_types,
                unique_key=self.unique_key,
                foreign_keys=self.foreign_keys,
                keys=self.keys
                ).query

        with DB(self.output_db) as database:
            drop_query = "DROP TABLE IF EXISTS `" + self.output_table + "`"
            database.execute(drop_query)
            database.execute(self.create_query)
                
    def add_main_source(self):
        self._get_data(None)
        
    def add_source(self, name, db, query, join_on, outer_join=False):
        """
        Adds a secondary source to the dictionary ('self.sources').
        
        - outer_join: If True, all the rows of the main source will be kept,
                      even if several of them doesn't match any row in this
                      source.
        
        - join_on:    The index of the column (in the main source) on which you
                      want to join this source (starting at 0). Thus, the
                      'join_on' column of the main source will be joined on
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
            
        with DB(source['db']) as db:
            if self.preliminary_query != None:
                db.execute(self.preliminary_query)
            
            data, cols_names, cols_types = db.execute(source['query'],
                                                      get_cols=True)
        
        # For the main source
        if source_name == None:
            source['data'] = data
            source['cols_names'] = cols_names
            source['cols_types'] = {n:t for n,t in zip(cols_names, cols_types)}
        
        # For an extra source
        else :
            source['data'] = get_dictionary(data)
            if len(cols_names) <= 1:
                raise NoColumnToJoinError(source_name)
            else:
                source['cols_names'] = cols_names[1:]
                source['cols_types'] = {n:t for n,t in zip(cols_names[1:],
                                                           cols_types[1:])}
                try:
                    source['join_on'] = self.main_source['cols_names'].index(source['join_on'])
                except Exception as e:
                    raise UnknownJoinOnColumnError(source['join_on'], source_name)
        
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
        
        # Checking if user defined column names are right
        cols_user_def = set(self.result_cols_names)
        cols_from_sql = set(self.result_cols_types.keys())
        cols_user_def_correct = cols_user_def & cols_from_sql
        
        if cols_user_def <= cols_from_sql:
            self.result_cols_names += tuple(cols_from_sql - cols_user_def)
        else:
            raise WrongColumnsNamesError(cols_user_def - cols_from_sql,
                                         cols_from_sql - cols_user_def)
               
    def _append_result_row(self, row, matches):
        """
        Given a row (of the main source) and the dictionary of the matches for
        each source, returns the final joined row.
        
        """
        result_row_unordered = {}
        result_row = []
        checks = [matches[match][0] != None or self.sources[match]['outer_join'] for match in matches]
        
        if all(checks):
            # Adding all the fields from the main source
            result_row_unordered.update(zip(self.main_source['cols_names'],
                                            row))
            
            # Concatenating the joined fields
            for s_name,s in self.sources.items():
                result_row_unordered.update(zip(s['cols_names'],
                                                matches[s_name]))
            
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
        self.add_main_source()
        for query_name, extra_query in extra_queries.items():
            self.add_source(name=query_name, **extra_query)
        
        self.join()
        self.write(rebuild=True)
        self.reporting()
        