"""
A class to implement simple group by functionality in Python.

"""

class GroupBy(object):
    group_by_functions = ['sum', ]
    
    def __init__(self, data_input, group_by):
        """
        data_input is the result of a SQL select query.
        
        group_by is a dictionary, e.g.:
        
        > group_by = {
        >     'indexes': [0, 6], # The columns to group_by.
        >     'functions': {
        >         'sum': [2, 3, 4],
        >     },
        > }
        
        indexes is required, and at least one of group_by_functions.
        
        """
        self.indexes = group_by['indexes']
        self.functions = group_by['functions']
        self.data_input = data_input
        self.data_output = []
        self.subgroups = []
    
    def _find_set(self, indexes):
        """
        Reduce the data input down to unique values.
        """
        hashable_input_data = [tuple([i[j] for j in indexes]) for i in self.data_input]
        self.group_keys = list(set(hashable_input_data))
    
    def sum(self, values):
        return sum(values)
    
    def _create_output(self):
        for group in self.subgroups:
            output = [group[0][i] for i in self.indexes]
            for function_type, indexes in self.functions.iteritems():
                for index in indexes:
                    values = []
                    for row in group:
                        values.append(row[index])
                    group_by_function = getattr(self, function_type)
                    output.append(group_by_function(values))
            self.data_output.append(output)
    
    def _group_input_data(self):
        for group_key in self.group_keys:
            self.subgroups.append([i for i in self.data_input if [i[x] for x in self.indexes] == list(group_key)])
    
    def process(self):
        self._find_set(self.indexes)
        self._group_input_data()
        self._create_output()
        return self.data_output