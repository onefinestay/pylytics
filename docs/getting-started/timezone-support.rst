Timezone support
================

In a typical implementation of pylytics, you might have a CRON job which runs at midnight UTC time, which executes all of your fact scripts.

However, problems arise when you need to take account of timezones. In an international business, you might want to record the sales for each country at midnight local time.

One solution would be to write several facts (one for each country), which are triggered by separate CRON jobs. This is satisfactory for small numbers of countries, but soon gets cumbersome.

Pylytics has built-in support for timezones, which cuts down on this repetition.

dim_timezone
************

When you create a new pylytics project, a `dim_timezone` dimension is automatically added to the `dim` folder. For existing projects, you can find `dim_timezone` in the `pylytics/conf/project_template/dim` folder of the source code.

What it does
------------

dim_timezone will create a table with all of the `Olson tz database <http://en.wikipedia.org/wiki/Tz_database>`_ timezones in it. The timezone information is accessed using `pytz <https://pypi.python.org/pypi/pytz/>`_.

Here is an example of a fact which uses timezone support::

    class FactBookingsCreated(Fact):
        """Records the number of bookings per location per day."""

        source_db = 'ecommerce'

        source_query = """
            SELECT
                CASE location
                    WHEN "LON" THEN "Europe/London"
                    WHEN "NYC" THEN "America/New_York"
                    WHEN "PAR" THEN "Europe/Paris"
                END AS dim_timezone,
                count(*) AS bookings_created
            FROM bookings
            WHERE CAST(creation_time as DATE) = CURDATE()
            GROUP BY location
            """

        dim_names = ['dim_timezone']
        dim_fields = ['timezone']

We included `dim_timezone` like any other dimension. The value returned from the source query for the `dim_timezone` column has to be in `tz database format <http://en.wikipedia.org/wiki/List_of_tz_database_time_zones>`_.

When the fact runs, only a subset of the `dim_timezone` rows will be exposed. These correspond to the timezones which are currently at midnight, local time (taking account of summer time).

This means that the only rows which will be inserted into the fact table are those which have a `dim_timezone` value corresponding to one of these timezones.

When you need timezone support, a CRON job should be setup to call ``manage.py name_of_my_timezone_fact.py update`` every half hour. This means that no timezones will be missed.
