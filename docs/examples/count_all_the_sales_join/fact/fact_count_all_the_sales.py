from pylytics.library.fact import Fact


class FactCountAllTheSales(Fact):
    
    source_db = 'test'  # A database defined in settings.py
    
    dim_names = ['dim_date', 'dim_location', None, None]
    dim_fields = ['date', 'location', None, None]

    source_query = """
        SELECT
            `sale_date` as `date`,
            `location_name` AS `location`,
            SUM(`sale_value`) AS `total_sales`
        FROM
            `sales`
        GROUP BY
            `location`, `date`;
        """
    
    extra_queries = ['manager']

    managers = {
        'db': 'test', # This would normally be a different database to source_db
        'query': """
            SELECT
               `location_name`,
               `manager`
            FROM `sales`
            """,
        'join_on': 1,
        'outer_join': False
        }
