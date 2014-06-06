from connection import DB
from table import Table


class Dim(Table):
    """Dimension base class."""

    def __init__(self, *args, **kwargs):
        super(Dim, self).__init__(*args, **kwargs)
        self.dim_or_fact = 'dim'

    def get_dictionary(self, field_name):
        """
        Returns a mapping of field values to their id.

        For example:
        {
            'field value 1': id_1,
            'field value 2': id_2,
            ...
        }

        """
        sql = """\
        SELECT {field_name}, {surrogate_key_column}
        FROM {table_name}
        ORDER BY {surrogate_key_column}
        """.format(field_name=field_name, table_name=self.table_name,
                   surrogate_key_column=self.surrogate_key_column)
        return dict(self.connection.execute(sql))

    def _transform_tuple(self, src_tuple):
        """
        Given a tuple representing a row of the source table (queried with
        self.source_query), returns a tuple representing a row of the dimension
        table to insert.
        NB: - This function should be implemented when extending the dim
              object.
            - The columns in the returned tuple must be in the same order as in
              the dimension table.
            - The first field (auto_increment `id`) and the last field
              (`created` automatic timestamp) must be omitted in the result.

        Example usage for a dimension table like (id, name, attrib, created):
        > _transform_tuple(('name_val_in', 'attrib_val_in', 'unused value'))
        Returns:
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
            query = "INSERT IGNORE INTO `{}` VALUES (NULL, {}, NULL)".format(
                self.table_name,
                self._values_placeholder(len(destination_tuple)),
                )
            self.connection.execute(query, destination_tuple)

        self.connection.commit()
