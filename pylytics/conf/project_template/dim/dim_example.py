from pylytics.library.dim import Dim


class DimExample(Dim):
    """Example Dimension."""

    source_db = 'example'  # The database to run this source query on.

    source_query = """
        SELECT DISTINCT `foo` FROM `bar`;
        """
