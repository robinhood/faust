import pytest
from yarl import URL
from faust.utils.urls import ensure_scheme, urllist


@pytest.mark.parametrize('default,url,expected', [
    (None, 'kafka://', URL('kafka://')),
    ('', 'kafka://', URL('kafka://')),
    ('http', 'localhost', URL('http://localhost')),
])
def test_ensure_scheme(default, url, expected):
    assert ensure_scheme(default, url) == expected


def test_urllist_URL():
    assert urllist(URL('foo://localhost')) == [URL('foo://localhost')]


@pytest.mark.parametrize('value', [
    None,
    '',
])
def test_urllist_empty_raises(value):
    with pytest.raises(ValueError):
        urllist(value)


def test_urllist_str():
    assert urllist('foo://localhost') == [URL('foo://localhost')]


def test_urllist_str_no_scheme():
    assert urllist('bar.com', default_scheme='foo') == [URL('foo://bar.com')]


def test_urllist_URL_no_scheme():
    assert urllist(URL('bar.com'), default_scheme='foo') == [
        URL('foo://bar.com'),
    ]


def test_urllist_strsep():
    assert urllist('foo://localhost;bar.com;example.com') == [
        URL('foo://localhost'),
        URL('foo://bar.com'),
        URL('foo://example.com'),
    ]


def test_urllist_strsep_no_scheme():
    assert urllist('localhost;bar.com;example.com', default_scheme='bar') == [
        URL('bar://localhost'),
        URL('bar://bar.com'),
        URL('bar://example.com'),
    ]


def test_urllist_URLs():
    assert urllist([
        URL('foo://localhost'),
        URL('bar.com'),
        URL('example.com'),
    ]) == [
        URL('foo://localhost'),
        URL('foo://bar.com'),
        URL('foo://example.com'),
    ]


def test_urllist_URLs_no_scheme():
    assert urllist([
        URL('localhost'),
        URL('bar.com'),
        URL('example.com'),
    ], default_scheme='foo') == [
        URL('foo://localhost'),
        URL('foo://bar.com'),
        URL('foo://example.com'),
    ]
