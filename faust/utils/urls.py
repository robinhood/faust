"""URL utilities - Working with URLs."""
from typing import List, Optional, Union
from yarl import URL

URIListArg = Union[str, URL, List[URL], List[str]]


def urllist(arg: URIListArg, *,
            default_scheme: str = None) -> List[URL]:
    """Create list of URLs.

    You can pass in a comma-separated string, or an actual list
    and this will convert that into a list of :class:`yarl.URL` objects.
    """
    if not arg:
        raise ValueError('URL argument cannot be falsy')

    if isinstance(arg, URL):
        # handle scalar URL argument.
        arg = [arg]
    elif isinstance(arg, str):
        # Handle scalar str, including semi-colon separated lists of URLs.
        urls = arg.split(';')

        # When some of the URLs do not have a scheme, we use
        # the first scheme we find as the default scheme.
        # For example:
        #    a:9092;kafka://b:9092;c:9092;d:9092
        # turns into
        #    kafka://a:9092
        #    kafka://b:9092
        #    kafka://c:9092
        #    kafka://d:9092
        #
        # we need to do this manually as the yarl.URL library
        # parses 'a:9092' as 'a://None:9092' so it thinks a is the
        # scheme and not the hostname.
        default_scheme = _find_first_actual_scheme(urls, default_scheme)
        arg = [_prepare_str_url(u, default_scheme) for u in urls]

    scheme = URL(arg[0]).scheme or default_scheme
    return [_ensure_scheme(scheme, URL(u)) for u in arg]


def _find_first_actual_scheme(
        urls: List[str],
        default_scheme: str = None) -> Optional[str]:
    for url in urls:
        scheme, sep, rest = url.partition('://')
        if sep:
            return scheme
    return default_scheme


def _prepare_str_url(s: str, default_scheme: str = None) -> str:
    # yarl.URL parses b:9092 into scheme=b,port=9092
    # where we would expect it to be scheme=None,host=b,port=9092
    if default_scheme:
        if '://' not in s:
            return f'{default_scheme}://{s}'
    return s


def _ensure_scheme(default_scheme: Optional[str], url: Union[URL]) -> URL:
    """Ensure URL has a default scheme.

    An URL like "localhost" will be returned with the default scheme
    added, while an URL with existing scheme is returned unmodified.
    """
    if default_scheme and not url.scheme:
        if url.is_absolute():
            return url.with_scheme(default_scheme)
        else:
            return URL(f'{default_scheme}://{url}')
    return url
