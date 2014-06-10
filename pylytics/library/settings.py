from importlib import import_module
from inspect import getmembers


class Settings(object):
    """ Generic settings structure for storing settings loaded
    from a Python module.
    """

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
