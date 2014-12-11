Creating a new pylytics project
===============================

Create a virtualenv and activate it. Then install pylytics::

    pip install pylytics

This installation adds `pylytics-admin.py` to the path. Use this to create a new pylytics project::

    pylytics-admin.py my_project_name

This creates the my_project_name folder in the current directory, with a skeleton project inside.

Project structure
-----------------

The project structure is as follows::

    __init__.py
    fact/
        __init__.py
        example_project
            __init__.py
            extract.py
            transform.py
            load.py
    dimension/
        __init__.py
        example_project
            __init__.py
            extract.py
            transform.py
            load.py
    shared/
        __init__.py
    manage.py
    settings.py
    client.cnf


File purposes
~~~~~~~~~~~~~

 * extract.py - contains all functions for pulling the raw data.
 * transform.py - contains functions making up the 'expansion pipeline', which cleans and expands upon the raw data.
 * load.py - contains fact and dimension definitions.


Why are facts and dimensions separated?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It's likely that your dimensions will be shared across several facts, which is why they're in separate folders.

However, there is no problem with declaring your dimensions in the same file as your facts.

The only constraint on project structure which must be followed is the facts you want to make available to the manage.py script have to be imported in fact/__init__.py. 

