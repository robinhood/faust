import pytest
from faust.sensors.statsd import StatsdMonitor

PREFIX = 'faust-test'


@pytest.fixture()
def mon():
    return StatsdMonitor(prefix=PREFIX)


def stream_from_channel(app):
    return app.stream(app.channel())


def stream_from_topic(app):
    return app.stream(app.topic('withdrawals'))


def stream_from_multiple_topics(app):
    return app.stream(app.topic('foo', 'bar'))


def stream_from_combined_streams(app):
    return app.stream(app.topic('foo')) & app.stream(app.topic('bar'))


@pytest.mark.parametrize('make_stream,expected', [
    (stream_from_channel, 'channel_anon'),
    (stream_from_topic, 'topic_withdrawals'),
    (stream_from_multiple_topics, 'topic_foo,bar'),
    (stream_from_combined_streams, 'topic_foo&topic_bar'),
])
def test_stream_label(make_stream, expected, *, app, mon):
    assert mon._stream_label(make_stream(app)) == expected
