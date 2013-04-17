from library.dim import Dim
import environment as SETTINGS

class DimLocation(Dim):
    source_db = SETTINGS.pylytics_db
    source_query = "SELECT DISTINCT `location` FROM `dim_location`"
