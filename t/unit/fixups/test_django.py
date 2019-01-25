import pytest
from faust.fixups import fixups
from faust.fixups.django import Fixup
from mode.utils.mocks import Mock, mask_module, patch_module


class test_Fixup:

    @pytest.fixture()
    def fixup(self, *, app):
        return Fixup(app)

    def test_fixup_env_enabled(self, *, app, fixup, monkeypatch):
        monkeypatch.setenv('DJANGO_SETTINGS_MODULE', 'settings')
        with patch_module('django'):
            assert fixup.enabled()
            assert any(isinstance(f, Fixup) for f in fixups(app))

    def test_fixup_env_enabled_no_django(self, *, app, fixup, monkeypatch):
        monkeypatch.setenv('DJANGO_SETTINGS_MODULE', 'settings')
        with mask_module('django'):
            with pytest.warns(UserWarning):
                assert not fixup.enabled()
                assert not any(isinstance(f, Fixup) for f in fixups(app))

    def test_fixup_env_disabled(self, *, app, fixup, monkeypatch):
        monkeypatch.delenv('DJANGO_SETTINGS_MODULE', raising=False)
        assert not fixup.enabled()
        assert not any(isinstance(f, Fixup) for f in fixups(app))

    def test_autodiscover_modules(self, *, fixup):
        with patch_module('django.apps'):
            from django.apps import apps
            config1 = Mock()
            config1.name = 'config1'
            config2 = Mock()
            config2.name = 'config2'
            apps.get_app_configs.return_value = [config1, config2]

            assert fixup.autodiscover_modules() == ['config1', 'config2']

    def test_on_worker_init(self, *, fixup):
        with patch_module('django',
                          'django.conf',
                          'django.core',
                          'django.core.checks'):
            import django
            from django.conf import settings
            from django.core.checks import run_checks

            settings.DEBUG = False
            fixup.on_worker_init()
            django.setup.assert_called_once_with()
            run_checks.assert_called_once_with()

            settings.DEBUG = True
            with pytest.warns(UserWarning):
                fixup.on_worker_init()

    def test_apps(self, *, fixup):
        with patch_module('django', 'django.apps'):
            from django.apps import apps
            assert fixup.apps is apps

    def test_settings(self, *, fixup):
        with patch_module('django', 'django.conf'):
            from django.conf import settings
            assert fixup.settings is settings
