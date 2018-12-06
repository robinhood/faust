import random
import socket
from typing import Iterable, NamedTuple, Union, cast
from mode.utils.logging import get_logger
from yarl import URL

__all__ = ['HostPort', 'collect_hosts', 'addrs_by_name', 'addrs_by_url']

logger = get_logger(__name__)

URLArg = Union[str, URL, Iterable[Union[str, URL]]]


class HostPort(NamedTuple):
    host: str
    port: int


def collect_hosts(url: URLArg,
                  default_port: int, *,
                  shuffle: bool = True) -> Iterable[HostPort]:
    """Resolve the hosts in a comma-separated URL string."""
    hosts = list(addrs_by_url(url, default_port))
    if shuffle:
        random.shuffle(hosts)
    return hosts


def addrs_by_name(host: str, port: int) -> Iterable[HostPort]:
    """Iterate over the IP addresses a host and port resolves to."""
    try:
        res = socket.getaddrinfo(host.strip(), port, 0, 0, socket.IPPROTO_TCP)
        for host_info in res:
            yield HostPort(host_info[4][0], host_info[4][1])
    except socket.gaierror as exc:
        logger.warning('Cannot resolve: %s: %r', host.strip(), exc)


def addrs_by_url(url: URLArg, default_port: int) -> Iterable[HostPort]:
    """Iterate over the IP addresses an URL resolves to as host/port tuples."""
    for host, port in expand_url(url, default_port):
        yield from addrs_by_name(host, port)


def expand_url(urls: Union[str, Iterable[str]],
               default_port: int) -> Iterable[HostPort]:
    """Iterate over host/port pairs from a list of URLs.

    The list of urls can be an actual list::

        >>> list(expand_url(['kafka://foo:9092', kafka://bar'], 9092))
        [HostPort('foo', 9092), HostPort('bar', 9092)]

    or a comma-separated string of urls::

        >>> list(expand_url('kafka://foo:9092,kafka://bar', 9092))
        [HostPort('foo', 9092), HostPort('bar', 9092)]

    or a single URL string::

        >>> list(expand_url('kafka://foo:9092', 9092))
        [HostPort('foo', 9092)]
    """
    if isinstance(urls, str):
        items = urls.split(',')
    elif isinstance(urls, URL):
        items = str(urls).split(',')
    else:
        items = cast(Iterable[str], urls)
    return [url_to_host_port(url, default_port) for url in items]


def url_to_host_port(url: str, default_port: int) -> HostPort:
    """Convert URL to tuple of host and port."""
    if '://' not in url:
        url = 'xxx://' + url
    u = URL(url)
    return HostPort(u.host, u.port or default_port)
