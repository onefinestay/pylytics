"""
A class to implement simple group by functionality in Python.

"""

class GroupBy(object):
    def __init__(self, data_input, indexes, sum_columns={}):
        self.indexes = indexes
        self.sum_columns = sum_columns
        self.data_input = data_input
        self.data_output = []
        self.subgroups = []
    
    def _find_set(self, indexes):
        """
        Reduce the data input down to unique values.
        """
        hashable_input_data = [tuple([i[j] for j in indexes]) for i in self.data_input]
        values = set(hashable_input_data)
        return list(values)
    
    def _sum_columns(self):
        for group in self.subgroups:
            output = [group[0][i] for i in self.indexes]
            for column_index in self.sum_columns.values():
                total = 0
                for row in group:
                    total += row[column_index]
                output.append(total)
            self.data_output.append(output)
    
    def sum(self):
        group_keys = self._find_set(self.indexes)
        for group_key in group_keys:
            self.subgroups.append([i for i in self.data_input if [i[x] for x in self.indexes] == list(group_key)])
        self._sum_columns()
        return self.data_output
