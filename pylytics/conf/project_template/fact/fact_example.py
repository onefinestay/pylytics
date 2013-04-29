from pylytics.library.fact import Fact


class FactExample(Fact):
    """Example Fact."""
    source_db = 'example'  # The database to run this source query on.
    source_query = """
        SELECT DISTINCT `foo` FROM `bar`;
        """
