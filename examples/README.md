To run these examples, follow the docs for creating a new pylytics project, and copy in settings.py and the dim and fact folders from the example folder.

Import the ``setup.sql`` file into your local MySQL database, which creates the relevant schema and sample data.

Then run:

```./manage.py all update```

This creates and updates the star schema tables.

Congratulations! You just created your first star schema.
