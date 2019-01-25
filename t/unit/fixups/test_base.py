from faust.fixups.base import Fixup


class test_Fixup:

    def test_init(self, *, app):
        assert Fixup(app).app is app

    def test_enabled(self, *, app):
        assert not Fixup(app).enabled()

    def test_autodiscover_modules(self, *, app):
        assert Fixup(app).autodiscover_modules() == []

    def test_on_worker_init(self, *, app):
        Fixup(app).on_worker_init()
