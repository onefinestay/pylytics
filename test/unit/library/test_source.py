from contextlib import closing

from pylytics.library.column import NaturalKey
from pylytics.library.dimension import Dimension
from pylytics.library.source import CallableSource
from pylytics.library.warehouse import Warehouse

################################################################################

def add_surname(row):
    row['name'] = row['name'] + ' Flintstone'


class Person(Dimension):
    __source__ = CallableSource.define(
        _callable=staticmethod(lambda: [
            {'name': 'Fred'},
            {'name': 'Wilma'},
            {'name': 'Pebbles'}
        ]),
        expansions=[add_surname]
    )
    name = NaturalKey('name', basestring)

################################################################################

class TestExpansions(object):

    def _get_rows(self):
        connection = Warehouse.get()
        with closing(connection.cursor(dictionary=True)) as cursor:
            cursor.execute('SELECT * FROM %s' % Person.__tablename__)
            rows = cursor.fetchall()
        return rows

    def test_expansions(self, empty_warehouse):
        """ Make sure the expansions are being called, and are manipulating
        the data.
        """
        Person.build()
        Person.update()
        rows = self._get_rows()
        names = [str(i['name']) for i in rows]
        assert ('Fred Flintstone' in names and
                'Wilma Flintstone' in names and
                'Pebbles Flintstone' in names)
