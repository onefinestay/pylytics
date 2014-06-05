from pylytics.library.dim import Dim


class DimLocation(Dim):
    """ Locations from Middle Earth
    """
    source_db = "middle_earth"
    source_query = "SELECT code, name FROM locations"
