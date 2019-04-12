"""URL utilities - Working with URLs."""
from typing import List, Optional, Union
from yarl import URL


def urllist(arg: Union[URL, str, List[str], List[URL]], *,
            default_scheme: str = None) -> List[URL]:
    """Create list of URLs.

    You can pass in a comma-separated string, or an actual list
    and this will convert that into a list of :class:`yarl.URL` objects.
    """
    if not arg:
        raise ValueError('URL argument cannot be falsy')

    if isinstance(arg, URL):
        arg = [arg]
    elif isinstance(arg, str):
        arg = arg.split(';')

    scheme = URL(arg[0]).scheme or default_scheme
    return [_ensure_scheme(scheme, URL(u)) for u in arg]


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
