from importlib import import_module
from inspect import getmembers


class Settings(object):
    """ Generic settings structure for storing settings loaded
    from a Python module.
    """

    # This list describes - in order - the settings modules to
    # attempt to load. There are listed 'settings' modules
    # (which should not be committed to the code repository)
    # and 'default_settings' modules (which should). The former
    # provides local overrides for settings within the latter
    # and modules with a broader scope are additionally inserted
    # after those with a narrower scope.
    #
    # The order of settings modules listed below is important
    # and should be maintained. None, some or all of these
    # modules may exist.
    #
    modules = [
        "test.settings",          # local test settings
        "test.default_settings",  # default test settings
        "settings",               # local application settings
        "default_settings",       # default application settings
    ]

    @classmethod
    def load_all(cls):
        """ Load settings from all modules listed in `modules`.
        """
        return cls.load(*cls.modules)

    @classmethod
    def load(cls, *modules):
        """ Load settings from one or more settings modules.

        This routine will attempt to load each settings module
        passed in as an argument, skipping those that cannot
        be imported. Each settings module will be loaded in
        turn and appended to a chain before being returned as
        a single Settings object.

        """
        settings = cls()
        for module_name in modules:
            try:
                module = import_module(module_name)
            except ImportError:
                pass
            else:
                members = {name.lower(): value
                           for name, value in getmembers(module)
                           if not name.startswith("_")}
                settings.append(Settings(**members))
        return settings

    def __init__(self, **settings):
        self.__chain = []
        if settings:
            self.__chain.append({key.lower(): value
                                 for key, value in settings.items()})

    def __getitem__(self, key):
        key = key.lower()
        for item in self.__chain:
            try:
                value = item[key]
            except KeyError:
                continue
            else:
                return value
        else:
            return None

    def __getattr__(self, key):
        return self.__getitem__(key)

    def append(self, other):
        """ Add settings to the end of the chain.
        """
        self.__chain = self.__chain + other.__chain

    def prepend(self, other):
        """ Add settings to the start of the chain.
        """
        self.__chain = other.__chain + self.__chain
