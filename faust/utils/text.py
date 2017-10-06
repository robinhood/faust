__all__ = ['title']


def title(s: str) -> str:
    """Capitalize sentence.

    ``"foo bar" -> "Foo Bar"``

    ``"foo-bar" -> "Foo Bar"
    """
    return ' '.join(
        p.capitalize()
        for p in s.replace('-', ' ')
                  .replace('_', '').split())
