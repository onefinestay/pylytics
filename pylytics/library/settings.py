# -*- encoding: utf-8 -*-

from __future__ import unicode_literals

import imp
import logging
import os
from importlib import import_module
from inspect import getmembers

from log import bright_black, bright_yellow


log = logging.getLogger("pylytics")


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
        "settings",                         # local application settings
        "default_settings",                 # default application settings
        "pylytics.library.system_settings", # sytem settings (rarely overriden)
    ]

    if os.environ.get('PYLYTICS_TEST', '0') == '1':
        modules = [
            "test.settings",          # local test settings
            "test.default_settings",  # default test settings
        ] + modules

    # Singleton instance.
    __instance = None

    @classmethod
    def get_instance(cls):
        """ Get singleton instance of Settings class. This is
        lazily instantiated and will attempt to load all modules
        listed in `modules`.
        """
        if cls.__instance is None:
            cls.__instance = cls.load(cls.modules)
        return cls.__instance

    @classmethod
    def load(cls, modules, from_path=False):
        """ Load settings from one or more settings modules.

        This routine will attempt to load each settings module
        passed in as an argument, skipping those that cannot
        be imported. Each settings module will be loaded in
        turn and appended to a chain before being returned as
        a single Settings object.

        """
        log.debug("Attempting to load settings from %s", ", ".join(modules))
        inst = cls()
        for module_name in modules:
            try:
                if from_path:
                    path, filename = os.path.split(module_name)
                    module = imp.load_source(filename.split('.')[0], path)
                else:
                    module = import_module(module_name)
            except ImportError:
                log.debug("[%s] No settings found for '%s'", bright_black('✗'),
                    module_name)
            else:
                members = {name.lower(): value
                           for name, value in getmembers(module)
                           if not name.startswith("_")}
                inst.append(Settings(**members))
                message = log.info("[%s] Settings loaded for '%s' from %s",
                    bright_yellow('✓'), module_name, module.__file__)
        return inst

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


# Export singleton Settings instance.
settings = Settings.get_instance()
