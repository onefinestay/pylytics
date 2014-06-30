from connection import DB
from table import Table, SourceData


class Dim(Table):
    """Dimension base class."""

    INSERT = """\
    INSERT IGNORE INTO `{table}` VALUES (NULL, {values}, NULL)
    """
    SELECT_DICT = """\
    SELECT `{field}`, `{surrogate_key}` FROM `{table}`
    ORDER BY `{surrogate_key}`
    """

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
        sql = self.SELECT_DICT.format(field=field_name, table=self.table_name,
                                      surrogate_key=self.surrogate_key_column)
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

    def _fetch_from_source(self, *args, **kwargs):
        """ Fetch data from a SQL data source as described by the `source_db`
        and `source_query` attributes.
        """
        self.log_info("Fetching rows from data source '%s'", self.source_db)
        with DB(self.source_db) as database:
            return SourceData(rows=database.execute(self.source_query))

    def _insert(self, data):
        """ Insert rows from the supplied `SourceData` instance into the table.
        """
        self.log_info("Inserting %s rows", len(data))
        connection = self.connection
        for row in data.rows:
            destination_tuple = self._transform_tuple(row)
            values = self._values_placeholder(len(destination_tuple))
            connection.execute(self.INSERT.format(
                table=self.table_name, values=values), destination_tuple)
        connection.commit()
