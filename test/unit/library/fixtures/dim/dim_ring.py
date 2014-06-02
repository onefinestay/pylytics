from pylytics.library.dim import Dim


class DimRing(Dim):
    """ Details of the Rings of Power.
    """
    source_db = "middle_earth"
    source_query = "SELECT name FROM rings_of_power"
