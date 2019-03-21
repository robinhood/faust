"""URL utilities - Working with URLs."""
from typing import List, Optional, Union
from yarl import URL

__all__ = ['urllist', 'ensure_scheme']


def urllist(arg: Union[URL, str, List[URL]], *,
            default_scheme: str = None) -> List[URL]:
    """Create list of URLs.

    You can pass in a comma-separated string, or an actual list
    and this will convert that into a list of :class:`yarl.URL` objects.
    """
    if not arg:
        raise ValueError('URL list argument cannot be empty')
    if not isinstance(arg, list):
        urls = str(arg).split(';')
        scheme = URL(urls[0]).scheme or default_scheme
        return [ensure_scheme(scheme, u) for u in urls]
    else:
        scheme = arg[0].scheme or default_scheme
        return [ensure_scheme(scheme, u) for u in arg]


def ensure_scheme(default_scheme: Optional[str], url: Union[str, URL]) -> URL:
    """Ensure URL has a default scheme.

    An URL like "localhost" will be returned with the default scheme
    added, while an URL with existing scheme is returned unmodified.
    """
    scheme: Optional[str] = None
    if default_scheme:
        if isinstance(url, URL):
            scheme = url.scheme
        else:
            _scheme, has_scheme, _ = url.partition('://')
            scheme = _scheme if has_scheme else None
        if not scheme:
            return URL(f'{default_scheme}://{url}')
    return URL(url)
