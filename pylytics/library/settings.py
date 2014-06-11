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
    # and modules with a broader scope are loaded before
    # those with a narrower scope.
    #
    # The order of settings modules listed below is therefore
    # important and should be maintained. None, some or all
    # of these modules may exist.
    #
    modules = [
        "default_settings",       # default application settings
        "settings",               # local application settings
        "test.default_settings",  # default test settings
        "test.settings",          # local test settings
    ]

    @classmethod
    def load(cls):
        """ Load settings from one or more settings modules.

        This routine will attempt to load each settings module
        listed in `modules` in order, skipping those that
        cannot be imported. Each settings module will be loaded
        in turn and applied over the top of those previously
        loaded.

        """
        settings = cls()
        for module in cls.modules:
            try:
                more_settings = Settings.from_module(module)
            except ImportError:
                pass
            else:
                settings.update(more_settings)
        return settings

    @classmethod
    def from_module(cls, name, package=None):
        """ Import settings from a module.
        """
        module = import_module(name, package)
        members = {name: value for name, value in getmembers(module)
                   if not name.startswith("_")}
        return cls(**members)

    def __init__(self, **settings):
        self.__settings = {}
        for key, value in settings.items():
            key = key.lower()
            self.__settings[key] = value

    def __getitem__(self, key):
        return self.__settings.get(key.lower())

    def __getattr__(self, key):
        return self.__getitem__(key)

    def update(self, settings):
        """ Update settings with other settings
        """
        if not isinstance(settings, Settings):
            raise TypeError("Can only update Settings with other Settings")
        self.__settings.update(settings.__settings)
