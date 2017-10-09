from difflib import SequenceMatcher
from typing import Iterable, Iterator, NamedTuple, Optional

__all__ = [
    'FuzzyMatch',
    'title',
    'fuzzymatch',
    'fuzzymatch_iter',
    'fuzzymatch_best',
    'abbr',
    'abbr_fqdn',
    'shorten_fqdn',
    'pluralize',
]


class FuzzyMatch(NamedTuple):
    ratio: float
    value: str


def title(s: str) -> str:
    """Capitalize sentence.

    ``"foo bar" -> "Foo Bar"``

    ``"foo-bar" -> "Foo Bar"``
    """
    return ' '.join(
        p.capitalize()
        for p in s.replace('-', ' ')
                  .replace('_', '').split())


def fuzzymatch(needle: str, haystack: Iterable[str],
               *,
               min_ratio: float = 0.6) -> Iterator[str]:
    for match in fuzzymatch_iter(needle, haystack, min_ratio=min_ratio):
        yield match.value


def fuzzymatch_iter(needle: str, haystack: Iterable[str],
                    *,
                    min_ratio: float = 0.6) -> Iterator[FuzzyMatch]:
    """Fuzzy Match: Including actual ratio.

    Yields:
        FuzzyMatch: tuples of ``(ratio, value)``.
    """
    for key in iter(haystack):
        ratio = SequenceMatcher(None, needle, key).ratio()
        if ratio >= min_ratio:
            yield FuzzyMatch(ratio, key)


def fuzzymatch_best(needle: str, haystack: Iterable[str],
                    *,
                    min_ratio: float = 0.6) -> Optional[str]:
    'Fuzzy Match - Return best match only (single scalar value).'
    try:
        return sorted(
            fuzzymatch_iter(
                needle,
                haystack,
                min_ratio=min_ratio),
            reverse=True,
        )[0].value
    except IndexError:
        return None


def abbr(s: str, max: int, suffix: str = '...',
         words: bool = False) -> str:
    """Abbreviate word."""
    if words:
        return _abbr_word_boundary(s, max, suffix)
    return _abbr_abrupt(s, max, suffix)


def _abbr_word_boundary(s: str, max: int, suffix: str) -> str:
    # Do not cut-off any words, but means the limit is even harder
    # and we won't include any partial words.
    if len(s) > max:
        return suffix and (s[:max - len(suffix)] + suffix) or s[:max]
    return s


def _abbr_abrupt(s: str, max: int, suffix: str = '...') -> str:
    # hard limit (can cut off in the middle of a word).
    print((s, max, suffix))
    if max and len(s) >= max:
        return s[:max].rsplit(' ', 1)[0] + suffix
    return s


def abbr_fqdn(origin: str, name: str, *, prefix: str = '') -> str:
    if name.startswith(origin):
        name = name[len(origin) + 1:]
    return f'{prefix}{name}'


def shorten_fqdn(s: str, max: int = 32) -> str:
    if len(s) > max:
        module, _, cls = s.rpartition('.')
        module = abbr(module, max - len(cls) - 3, None, words=True)
        return module + '[.]' + cls
    return s


def pluralize(n: int, text: str, suffix: str = 's') -> str:
    """Pluralize term when n is greater than one."""
    if n != 1:
        return text + suffix
    return text
