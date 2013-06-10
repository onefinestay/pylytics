"""
This module allows you to get data from various SQL databases, "join" them
together within your Python script, and write the result into your own
database.

High-level usage:
    > from python_join import TableBuilder
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
    > from python_join import TableBuilder
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


from connection import DB
from datetime import datetime


###############################################################################

# Custom Exceptions

class SourceAlreadyExistsError(Exception):
    def __init__(self, value):
        self.value = value
    
    def __str__(self):
        return "Error while adding source '%s' : it already exists" % self.value


class WrongColumnCountError(Exception):
    def __init__(self, output_table_cols, sample_input_cols, table_name):
        self.output_table_cols = output_table_cols
        self.out_len = len(output_table_cols)
        self.sample_input_cols = sample_input_cols
        self.in_len = len(sample_input_cols)
        self.table_name = table_name
    
    def __str__(self):
        message = """
            - The data you are about to insert contains %s columns, whereas the `%s` table contains %s columns.
            Here is the list of the output table columns (on the left hand side), along with a sample data you are trying to insert (on the right hand side) :

        """ % (self.in_len, self.table_name, self.out_len)
        for o, i in zip(self.output_table_cols, self.sample_input_cols):
            message += "        %s = '%s'\n" % (o, i)
        
        if self.in_len > self.out_len:
            message += "".join(["       ? = "+e+"\n" for e in self.sample_input_cols[(self.out_len):]])
        else :
            message += "".join(["       "+e+" = ?\n" for e in self.output_table_cols[(self.in_len):]])
        
        return message


class NoColumnToJoinError(Exception):
    def __init__(self, source_name):
        self.source_name = source_name
    
    def __str__(self):
        return """The source '%s' contains less than 2 columns. All the \
        secondary sources should contain at least one column to join on, as \
        well as one column to add.""" % self.source_name


###############################################################################

class TableBuilder(object):
    """
    The main object you have to instantiate to join tables.
    
    """
    
    def __init__(self, main_db, main_query, create_query, output_table,
                 verbose=False, output_db='datawarehouse',
                 preliminary_query=None, transform_row=(lambda x: x)):
        self.sources = {}
        self.output_table = output_table
        self.create_query = create_query
        self.verbose = verbose
        self.result = []
        self._transform_row = transform_row
        self.output_db = output_db
        self.start_time = start_time = datetime.now()
        
        self.main_source = {
            'db':main_db,
            'query':main_query,
            'data':[]
        }
        self.preliminary_query = preliminary_query
        self._get_data(None)
    
    def _rebuild_sql(self):
        """
        Drops and rebuilds the output table.
        
        """
        if self.verbose:
            print "... (Re)-creating the output table ..."
        
        with DB(self.output_db) as dw:
            query1 = "DROP TABLE IF EXISTS `" + self.output_table + "`"
            query2 = self.create_query
            dw.execute(query1)
            dw.execute(query2)
    
    def add_source(self, name, db, query, join_on, outer_join=False):
        """
        Adds a secondary source to the dictionary ('self.sources')
        
        - outer_join: If True, all the rows of the main source will be kept,
                      even if several of them doesn't match any row in this
                      source.
        
        - join_on:    The index of the column (in the main source) on which you
                      want to join this source (starting at 0). Thus, the
                      'join_on'-th column of the main source will be joined on
                      the first (index 0) column of this source.

        """
        if name in self.sources or name=='main':
            raise SourceAlreadyExistsError(name)
        else :
            self.sources[name] = {
                'id':len(self.sources),
                'db':db,
                'query':query,
                'count_cols':None,
                'join_on':join_on,
                'outer_join':outer_join,
                'errors_count':0,
                'matches_count':0,
                'errors':[],
                'data':None
            }
        
        self._get_data(name)
    
    def _get_sources_order(self):
        """
        Returns a list of all the sources names ordered by their 'id' field.
        
        """
        ids = {s['id']:s_name for s_name,s in self.sources.items()}
        
        return [ids[e] for e in sorted(ids)]
    
    def _get_data(self, source_name):
        """
        Gets the data from a source and stores it
        
        'source_name' should be either the name provided in 'add_source()' or
        None to get data from the main source.
        
        """
        if self.verbose:
            print "... Getting data from '%s' source ..." % (source_name or
                                                             'main')
        
        if source_name == None:
            source = self.main_source
        else :
            source = self.sources[source_name]
        
        with DB(source['db']) as db:
            if self.preliminary_query != None:
                db.execute(self.preliminary_query)
            
            data = db.execute(source['query'], get_count_cols=True)
            
            if source_name == None:
                self.main_source['data'] = data[0]
            else :
                self.sources[source_name]['data'] = self._get_dictionary(data[0])
                if data[1] <= 1:
                    raise NoColumnToJoinError(source_name)
                else:
                    self.sources[source_name]['count_cols'] = data[1]-1
    
    def _get_dictionary(self, data):
        """
        Given a list of tuples, returns a dictionary where :
        - the key is the first element of each tuple
        - the value is the tuple without its first value
        
        Example :
            > self._get_dictionary([('a','b','c'),('x','y','z'),('u','v','w')]
            {'a':('b','c'), 'x':('y','z'), 'u':('v','w')}
        
        """
        dict = {}
        for e in data:
            dict[e[0]]=e[1:]
        return dict
    
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
        result_row = []
        sources_ordered = self._get_sources_order()
        
        if all([matches[m][0] != None or self.sources[m]['outer_join'] for
               m in matches]):
            
            # Adding all the fields from the main source
            result_row += row
            
            # Concatenating the joined fields
            for s_name in sources_ordered:
                result_row += matches[s_name]
            
            self.result.append(self._transform_row(result_row))
    
    def join(self):
        """
        Joins all the secondary sources to the main source
        (and stores the result in 'self.result').
        
        """
        matches = None
        
        if self.verbose:
            print "... Joining sources ..."
        
        for row in self.main_source['data']:
            matches = {}
            
            for s_name,s in self.sources.items():
                try:
                    matches[s_name] = s['data'][row[s['join_on']]]
                    s['matches_count'] += 1
                except Exception, e:
                    matches[s_name] = (None,)*s['count_cols']
                    s['errors'].append(row[s['join_on']])
                    s['errors_count'] += 1
            
            self._append_result_row(row, matches)
    
    def _get_columns(self, db_name, table):
        """
        Returns the list of the column names for the given table.
        
        """
        cols = None
        
        with DB(db_name) as dw:
            cur = dw.connection.cursor()
            cur.execute("SELECT * FROM `"+table+"` LIMIT 0,0")
            cols = cur.description
            cur.close()
        
        return [col[0] for col in cols]
    
    def _write_data_batches(self, query):
        """
        Executes the given insert query for each line of the 'self.result'
        data. The inserts are performed by batches of 1000.
        
        """
        N = 1000
        # L = ceil(len(self.result)+1)/N
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
        
        if self.verbose:
            print "... Writing the data into the datawarehouse ..."
        
        cols = self._get_columns(self.output_db, self.output_table)
        
        if len(self.result) > 0:
            # Checking if the data we are about to insert contain the same
            # number of columns as the output table:
            if len(cols) == len(self.result[0]):
                with DB(self.output_db) as dw:
                    query = "INSERT INTO " + self.output_table + " VALUES (" + ",".join(["%s"] * len(self.result[0])) + ")"
                    self._write_data_batches(query)
            else:
                self.reporting()
                raise WrongColumnCountError(cols, self.result[0], self.output_table)
        
        elif self.verbose:
                print "No data inserted"
    
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
        print "\n- Main source (from '%s') :    %s rows" % (
            self.main_source['db'], len(self.main_source['data']))
        
        for s_name, s in self.sources.items():
            print ""
            print "- Source '%s' :      %s rows %s matches  %s errors" % (s_name, len(s['data']), s['matches_count'], s['errors_count'])
            if self.verbose and s['errors_count'] > 0:
                print " * Keys not found : " + ("   ".join(map(lambda x: '"' + str(x) + '"', s['errors'])))
        print ""
        print "(Execution started at : %s)" % self.start_time
        print "(Execution time : %s)" % (datetime.now() - self.start_time)
    
    def quick_join(self, *args):
        """
        High-level function to use the library (takes a list of sources):
        gets the data, performs the join and writes the output.
        
        Example :
            > self.quick_join(
                {
                    'name':'...',
                    'db':'...',
                    'query':'...',
                    'join_on':'...'
                },
                {
                    'name':'...',
                    'db':'...',
                    'query':'...',
                    'join_on':'...',
                    'outer_join':True
                }
            )
        
        """
        for s in args:
            self.add_source(**s)
        
        self.join()
        self.write(rebuild=True)
        self.reporting()
        