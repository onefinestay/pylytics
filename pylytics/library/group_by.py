"""
A class to implement simple group by functionality in Python.

"""

class GroupBy(object):
    def __init__(self, data_input, indexes, sum_columns={}):
        self.indexes = indexes
        self.sum_columns = sum_columns
        self.data_input = data_input
        self.data_output = []
    
    def find_set(self, indexes):
        """
        Reduce the data input down to unique values.
        """
        hashable_input_data = [tuple([i[j] for j in indexes]) for i in self.data_input]
        values = set(hashable_input_data)
        return list(values)
    
    def sum(self):
        group_keys = self.find_set(self.indexes)
        subgroups = []
        
        for group_key in group_keys:
            # Append if the fields match the group_keys - possible to split on certain fields.
            subgroups.append([i for i in self.data_input if (i[x] for x in self.indexes) == group_key])
        
        import pdb; pdb.set_trace()
        
        
        
        
        
        
        
        return self.data_output
        