from library.dim import Dim

class DimLocation(Dim):
    source_db = 'example'
    source_query = "SELECT DISTINCT `location_name` FROM `locations`"
