import os

# make sure the gevent event loop is used as early as possible.
os.environ.setdefault('FAUST_LOOP', 'gevent')

# set the default Django settings module for the 'faust' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proj.settings')

import django  # noqa: E402
import faust  # noqa: E402
from django.apps import apps  # noqa: E402
from django.conf import settings


app = faust.App('django-proj')

@app.on_configured.connect
def configure_from_settings(app, conf, **kwargs):
    conf.broker = getattr(settings, 'FAUST_BROKER_URL', 'kafka://')
    conf.store = getattr(settings, 'FAUST_STORE_URL', 'rocksdb://')
    conf.autodiscover = [config.name for config in apps.get_app_configs()]


def main():
    from django.core.checks import run_checks
    django.setup()
    run_checks()
    app.main()


if __name__ == '__main__':
    main()
