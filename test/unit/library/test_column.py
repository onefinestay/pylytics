import pytest

from pylytics.library.column import Column


class TestColumnCreation(object):

    def test_cannot_create_a_column_with_an_odd_type(self):
        column = Column("foo", object)
        with pytest.raises(TypeError):
            _ = column.type_expression
