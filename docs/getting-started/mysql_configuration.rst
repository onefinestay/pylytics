MySQL Configuration
===================

Pylytics is tested against MySQL 5.5 and 5.6.

If you are running MySQL 5.6, make sure the server is configured as following::

    explicit_defaults_for_timestamp=OFF

This is the default setting for MySQL 5.6, however some cloud providers have it set differently.
