from contextlib import contextmanager
from unittest.mock import patch
import pytest
from faust.utils.sockets import HostPort, collect_hosts, expand_url


@contextmanager
def mock_addrinfo(**res):
    with patch('socket.getaddrinfo') as getaddrinfo:

        def on_getaddrinfo(host, port, *args, **kwargs):
            return [('A', 'B', 'C', 'D', (addr, port)) for addr in res[host]]

        getaddrinfo.side_effect = on_getaddrinfo

        yield getaddrinfo



@pytest.mark.parametrize('url,expected_list', [
    ('kafka://foo', [HostPort('foo', 9092)]),
    ('kafka://foo,kafka://bar:6666',
     [HostPort('foo', 9092), HostPort('bar', 6666)]),
    (['kafka://foo'], [HostPort('foo', 9092)]),
    (['kafka://foo', 'kafka://bar:6666'],
     [HostPort('foo', 9092), HostPort('bar', 6666)]),
    (['foo', 'bar:6666'],
     [HostPort('foo', 9092), HostPort('bar', 6666)]),
])
def test_expand_url(url, expected_list):
    assert list(expand_url(url, default_port=9092)) == expected_list



@pytest.mark.parametrize('url', [
    'kafka://foo,kafka://bar',
    ['kafka://foo', 'kafka://bar'],
    ['foo', 'bar'],
])
def test_collect_hosts(url):
    with mock_addrinfo(foo=['10.1.2.1', '10.1.2.3', '10.1.2.5'],
                       bar=['10.2.2.1', '10.2.2.3', '10.2.2.5']):
        res1 = list(collect_hosts(url, 9092, shuffle=False))
        assert res1 == [
            HostPort('10.1.2.1', 9092),
            HostPort('10.1.2.3', 9092),
            HostPort('10.1.2.5', 9092),
            HostPort('10.2.2.1', 9092),
            HostPort('10.2.2.3', 9092),
            HostPort('10.2.2.5', 9092),
        ]
        res2 = list(collect_hosts(url, 9092, shuffle=True))
        assert sorted(res2) == sorted(res1)
        assert res2 != res1
