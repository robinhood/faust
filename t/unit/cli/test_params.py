import click.exceptions
import pytest
from yarl import URL
from faust.cli.params import TCPPort, URLParam


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
