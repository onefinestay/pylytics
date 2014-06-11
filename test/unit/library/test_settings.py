import pytest

from pylytics.library.settings import Settings

from test.unit.library.fixtures import fake_settings


class TestSettings(object):

    def test_can_retrieve_settings_by_attribute(self):
        # when
        settings = Settings(king="Arthur", table="round")
        # then
        assert settings.king == "Arthur"
        assert settings.table == "round"

    def test_can_retrieve_settings_by_key(self):
        # when
        settings = Settings(king="Arthur", table="round")
        # then
        assert settings["king"] == "Arthur"
        assert settings["table"] == "round"

    def test_missing_settings_return_none(self):
        # when
        settings = Settings(king="Arthur", table="round")
        # then
        assert settings.dog is None

    def test_key_names_are_cast_to_lower_case(self):
        # when
        settings = Settings(King="Arthur", TABLE="round")
        # then
        assert settings.king == "Arthur"
        assert settings.table == "round"

    def test_can_import_settings_from_module(self):
        # given
        module_name = "test.unit.library.fixtures.fake_settings"
        # when
        settings = Settings.load(module_name)
        # then
        assert settings.pylytics_db == fake_settings.pylytics_db
        assert settings.databases == fake_settings.DATABASES

    def test_early_loaded_beats_late_loaded(self):
        # given
        settings = Settings(king="Arthur", queen="Guinevere")
        settings.append(Settings(king="Arthur Pendragon", table="round"))
        # then
        assert settings.king == "Arthur"
        assert settings.queen == "Guinevere"
        assert settings.table == "round"
