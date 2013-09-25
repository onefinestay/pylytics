from connection import DB
from table import Table


class Dim(Table):
    """Dimension base class."""

    def __init__(self, *args, **kwargs):
        self.dim_or_fact = 'dim'
        super(Dim, self).__init__(*args, **kwargs)

    def get_dictionary(self, field_name):
        """
        Returns a dictionary of the dimension.

        For example:
        {
            'field value 1':id1
            'field value 2':id2
            ...
        }

        """
        dictionary = {}
        data = self.connection.execute(
            "SELECT `%s`, id FROM `%s` order by id asc;" % (field_name, self.table_name)
            )
        for element in data:
            dictionary[element[0]] = element[1]

        return dictionary

    def _transform_tuple(self, src_tuple):
        """
        Given a tuple representing a row of the source table (queried with
        self.source_query), returns a tuple representing a row of the dimension
        table to insert.
        NB: - This function should be implemented when extending the dim
              object.
            - The columns in the retured tuple must be in the same order as in
              the dimension table.
            - The first field (auto_increment `id`) and the last field
              (`created` automatic timestamp) must be omitted in the result.

        Example usage for a dimension table like (id, name, attrib, created):
        > _transform_tuple(('name_val_in', 'attrib_val_in', 'unused value'))
        Returns :
        > ('name_val_out', 'attrib_val_out')

        """
        return src_tuple

    def update(self):
        """Updates the dimension table."""
        # Status.
        msg = "Populating %s" % self.table_name
        self._print_status(msg)

        # Get the full source list.
        data = []
        with DB(self.source_db) as database:
            data = database.execute(self.source_query)

        # Update the dim table.
        for row in data:
            destination_tuple = self._transform_tuple(row)
            self.connection.execute(
                """INSERT IGNORE INTO `%s` VALUES (NULL, %s, NULL)""" % (
                    self.table_name,
                    self._values_placeholder(len(destination_tuple))
                    ),
                destination_tuple
                )
