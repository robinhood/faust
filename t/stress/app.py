import faust
from . import config

__all__ = ['app']

app = faust.App(
    'faust-stress',
    broker=config.broker,
    store=config.store,
    origin='t.stress',
    topic_partitions=config.topic_partitions,
    loghandlers=config.loghandlers(),
    autodiscover=True,
)
