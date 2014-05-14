from pylytics.library.fact import Fact


class FactCountAllTheSales(Fact):

    source_db = 'test'  # A database defined in settings.py

    dim_names = ['dim_date', 'dim_location', None]
    dim_fields = ['date', 'location', None]

    source_query = """
        SELECT
            sale_date as `date`,
            location_name AS location,
            SUM(sale_value) AS total_sales
        FROM
            sales
        GROUP BY
            location, `date`;
        """

    def test(self):
        self.update()
        print "Test succeeded because I have chosen not to implement a test"
