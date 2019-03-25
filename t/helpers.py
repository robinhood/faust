from time import time
from faust.events import Event
from faust.types.tuples import Message

__all__ = ['message', 'new_event']


def message(key=None, value=None,
            *,
            topic='topic',
            partition=0,
            timestamp=None,
            headers=None,
            offset=1,
            checksum=None):
    return Message(
        key=key,
        value=value,
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=timestamp or time(),
        timestamp_type=1 if timestamp else 0,
        headers=headers,
        checksum=checksum,
    )


def new_event(app, key=None, value=None, *,
              headers=None,
              **kwargs):
    return Event(app, key, value, headers, message(
        key=key, value=value, headers=headers, **kwargs))
