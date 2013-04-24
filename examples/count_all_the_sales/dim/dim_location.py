from pylytics.library.dim import Dim
import settings


class DimLocation(Dim):
    source_db = settings.pylytics_db
    source_query = "SELECT DISTINCT `location` FROM `dim_location`"
