from pylytics.library.dim import Dim


class DimLocation(Dim):
    source_db = "test"  # A database defined in settings.py
    source_query = "SELECT DISTINCT `location_name` FROM `sales`"
