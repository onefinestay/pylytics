from pylytics.library.fact import Fact


class FactRingJourney(Fact):
    """ Example fact class
    """
    source_db = "middle_earth"
    source_query = """
        SELECT
            ring_name AS dim_ring,
            checkpoint AS dim_location,
            1 AS fellowship_count
        FROM ring_journey
        """

    dim_names = ["dim_ring", "dim_location"]
    dim_fields = ["name", "code"]
