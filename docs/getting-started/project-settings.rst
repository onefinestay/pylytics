Project settings
****************

Make sure that the DATABASES dictionary in settings.py contains the details for all the databases you need.

At the moment, only MySQL databases are supported.

`pylytics_db` specifies which of these connections is your datawarehouse, which will be used for inserting facts and dimensions into.

client.cnf
----------

This file is passed into the MySQL connector (Oracle's Python connector is used under the hood). It allows you to configure the MySQL connections you make from the client side. It's unlikely you'll need this, but it's useful for performance tweaking if required.
