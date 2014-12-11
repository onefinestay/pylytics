Visualisation Tools
===================

Mondrian
--------

pylytics can export an XML schema definition for Mondrian.

Mondrian is an open source MDX engine, which powers Pentaho, Jaspersoft, and Saiku. These are all Business Intelligence products, which can visualise the data stored in star schemas.

Before using these tools you need to define an XML schema, which tells Mondrian about the structure of your star schema.

To export the XML use the following command::

    ./manage.py template Fact_1

This will export an entire schema definition for that fact (including dimension definitions).

The XML should be double checked for accuracy, because pylytics can't completely second guess the end requirements, but it's still a big time saver.
