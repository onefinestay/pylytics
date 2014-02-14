Setup scripts
=============

In some situations it is useful to run scripts before the Facts are executed.

Examples are writing a Python script which performs some kind of notification, or accesses data from an API and stores in a SQL database so it's accessible from a Fact script.

Writing setup scripts
----------------------

Setup scripts live in the ``scripts`` folder, which is created by default at the root of a pylytics project.

Each script must define a ``main`` function, which is what will be executed.

As an example, if a setup script called ``pull_from_google_api`` is required, we have two ways of writing it.

Firstly, as a file called ``pull_from_google_api.py`` in the scripts folder, with contains a ``main`` function.

Secondly, as a folder called ``pull_from_google_api`` in the scripts folder, with an ``__init__.py`` file inside, which contains a ``main`` function.

Listing required setup scripts in your Facts
--------------------------------------------

You designate which setup scripts will run for each Fact by including the ``setup_scripts`` property::

    class MyFact(Fact):
        setup_scripts = {
            'update': ['pull_from_google_api'],
            'historical': ['pull_from_google_api'],
            'build': ['notify_admin'],
        }
        ...

``setup_scripts`` must be a dictionary. The keys are the pylytics commands (i.e. ``update``, ``historical``, ``build``). So in the example above, the ``pull_from_google_api`` script will be run before ``update`` or ``historical`` gets executed for ``MyFact``.

If you have several Facts, and each one defines the same script, then the script will only be run once before all Facts are run.
