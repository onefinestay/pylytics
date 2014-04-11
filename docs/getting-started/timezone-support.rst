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

dim_timezone will create a table with all the timezones in it.

.. TODO Might want to include more built-in dims in this way.
