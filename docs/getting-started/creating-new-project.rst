Creating a new pylytics project
===============================
Create a virtual environment and activate it. Then install pylytics::

    pip install pylytics

This installation adds pylytics-admin.py to the path. Use this to create a new pylytics project::

    pylytics-admin.py my_project_name

This creates the my_project_name folder in the current directory, with a skeleton project inside.

Project structure
-----------------

The project structure is as follows::

    __init__.py
    fact/
        __init__.py
        fact_example.py
        sql/
    dim/
        __init__.py
        dim_example.py
        sql/
    scripts/
        __init__.py
    manage.py
    settings.py
