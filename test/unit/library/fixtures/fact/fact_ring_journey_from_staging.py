from pylytics.library.fact import Fact


class FactRingJourneyFromStaging(Fact):
    """ Example fact class that draws from staging.
    """
    source_db = "__staging__"

    dim_names = ["dim_ring", "dim_location"]
    dim_fields = ["name", "code"]

    # We also need a list of metric names which will come in order
    # after the dimensions.
    metric_names = ["fellowship_count"]
