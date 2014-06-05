from pylytics.library.fact import Fact


class FactRingJourneyFromStaging(Fact):
    """ Example fact class that draws from staging.
    """
    source_db = "__staging__"

    dim_names = ["dim_ring", "dim_location"]
    dim_fields = ["name", "code"]
