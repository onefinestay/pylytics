Examples
========

To run these examples, make sure pylytics is installed, and on your path.

Make sure that MySQL is running locally, and there's a *pylytics_test* database created.

Import the *setup.sql* file into your *pylytics_test* database, which creates the relevant schema and sample data.

Then run the following from inside one of the example folders::

    python manage.py all update

This creates and updates the star schema tables.

Congratulations! You just created your first star schema.
