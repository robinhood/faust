from typing import List, Optional, Union
from yarl import URL

__all__ = ['urllist', 'ensure_scheme']


def urllist(arg: Union[URL, str, List[URL]], *,
            default_scheme: str = None) -> List[URL]:
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
