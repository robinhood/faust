import pytest
from faust.utils.urls import as_url, parse_url, maybe_sanitize_url


def test_parse_url():
    assert parse_url('amqp://user:pass@localhost:5672/my/vhost') == {
        'transport': 'amqp',
        'userid': 'user',
        'password': 'pass',
        'hostname': 'localhost',
        'port': 5672,
        'virtual_host': 'my/vhost',
    }


@pytest.mark.parametrize('urltuple,expected', [
    (('https',), 'https:///'),
    (('https', 'e.com'), 'https://e.com/'),
    (('https', 'e.com', 80), 'https://e.com:80/'),
    (('https', 'e.com', 80, 'u'), 'https://u@e.com:80/'),
    (('https', 'e.com', 80, 'u', 'p'), 'https://u:p@e.com:80/'),
    (('https', 'e.com', 80, None, 'p'), 'https://:p@e.com:80/'),
    (('https', 'e.com', 80, None, 'p', '/foo'), 'https://:p@e.com:80//foo'),
])
def test_as_url(urltuple, expected):
    assert as_url(*urltuple) == expected


@pytest.mark.parametrize('url,expected', [
    ('foo', 'foo'),
    ('http://u:p@e.com//foo', 'http://u:**@e.com//foo'),
])
def test_maybe_sanitize_url(url, expected):
    assert maybe_sanitize_url(url) == expected
    assert (maybe_sanitize_url('http://u:p@e.com//foo') ==
            'http://u:**@e.com//foo')
