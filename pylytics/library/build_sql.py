"""Automatic generation of SQL create queries."""

import os

from jinja2 import Template


class SQLBuilder(object):
    """
    A class to easily generate a MySQL CREATE query, knowing the columns names,
    types, unique key and foreign key contraints.
    """
    def __init__(self, table_name, cols_names, cols_types, unique_key=None,
                 foreign_keys=None, keys=None):
        self.table_name = table_name
        self.cols_names = cols_names
        self.cols_types = cols_types
        self.unique_key = unique_key
        self.foreign_keys = foreign_keys
        self.keys = keys
        self.query = self._get_query()

    def _get_query(self):
        """
        Builds and returns the CREATE query

        """
        columns = []
        for column in self.cols_names:
            columns.append('`{0}` {1}'.format(column,
                                                    self.cols_types[column]))

        template_path = os.path.join(os.path.dirname(__file__),
                                     'templates/create_table.jinja')
        with open(template_path, 'r') as sql_file:
            template_contents = sql_file.read()

        template = Template(template_contents, trim_blocks=True)

        rendered_template =  template.render(
            table_name = self.table_name,
            columns = columns,
            unique_key = self.unique_key,
            foreign_keys = self.foreign_keys,
            keys = self.keys)

        return rendered_template
