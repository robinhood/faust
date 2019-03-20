import click.exceptions
import pytest
from yarl import URL
from faust.cli.params import CaseInsensitiveChoice, TCPPort, URLParam


def test_CaseInsensitiveChoice():
    choices = CaseInsensitiveChoice([
        'FOO',
        'BAR',
        'baz',
    ])

    assert choices.convert('FOO', None, None) == 'FOO'
    assert choices.convert('foo', None, None) == 'foo'
    assert choices.convert('Foo', None, None) == 'Foo'
    assert choices.convert('BAZ', None, None) == 'BAZ'

    with pytest.raises(click.exceptions.BadParameter):
        choices.convert('xuz', None, None)


def test_TCPPort():
    port = TCPPort()
    assert port.convert(1, None, None) == 1
    assert port.convert(30, None, None) == 30
    assert port.convert(65535, None, None) == 65535
    with pytest.raises(click.exceptions.BadParameter):
        port.convert(0, None, None)
    with pytest.raises(click.exceptions.BadParameter):
        port.convert(-312, None, None)
    with pytest.raises(click.exceptions.BadParameter):
        port.convert(513412321, None, None)
    with pytest.raises(click.exceptions.BadParameter):
        port.convert(65536, None, None)


def test_URLParam():
    urlp = URLParam()
    assert repr(urlp) == 'URL'

    url = urlp.convert('http://foo.com/path/', None, None)
    assert isinstance(url, URL)
    assert url.host == 'foo.com'
