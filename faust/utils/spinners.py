"""Terminal progress bar spinners."""
import atexit
import random
import sys
from typing import IO, Sequence

SPINNER_ARC: Sequence[str] = [
    '◜', '◠', '◝', '◞', '◡', '◟',
]
SPINNER_ARROW: Sequence[str] = [
    '←', '↖', '↑', '↗', '→', '↘', '↓', '↙',
]
SPINNER_CIRCLE: Sequence[str] = [
    '•', '◦', '●', '○', '◎', '◉', '⦿',
]
SPINNER_SQUARE: Sequence[str] = [
    '◢', '◣', '◤', '◥',
]
SPINNER_MOON: Sequence[str] = [
    '🌑 ', '🌒 ', '🌓 ', '🌔 ', '🌕 ', '🌖 ', '🌗 ', '🌘 ',
]

SPINNERS: Sequence[Sequence[str]] = [
    SPINNER_ARC,
    SPINNER_ARROW,
    SPINNER_CIRCLE,
    SPINNER_SQUARE,
    SPINNER_MOON,
]

ACTIVE_SPINNER: Sequence[str] = random.choice(SPINNERS)


class Spinner:
    """Progress bar spinner."""

    bell = '\b'
    sprites: Sequence[str] = ACTIVE_SPINNER
    cursor_hide: str = '\x1b[?25l'
    cursor_show: str = '\x1b[?25h'
    hide_cursor: bool = True

    def __init__(self, file: IO = sys.stderr) -> None:
        self.file: IO = file
        self.width: int = 0
        self.count = 0

    def update(self) -> None:
        if not self.count:
            self.begin()
        i = self.count % len(self.sprites)
        self.count += 1
        self.write(self.sprites[i])

    def write(self, s: str) -> None:
        if self.file.isatty():
            self._print(f'{self.bell * self.width}{s.ljust(self.width)}')
            self.width = max(self.width, len(s))

    def _print(self, s: str) -> None:
        print(s, end='', file=self.file)
        self.file.flush()

    def begin(self) -> None:
        atexit.register(type(self)._finish, self.file, at_exit=True)
        self._print(self.cursor_hide)

    def finish(self) -> None:
        print(f'{self.bell * (self.width + 1)}', end='', file=self.file)
        self._finish(self.file)

    @classmethod
    def _finish(cls, file: IO, *, at_exit: bool = False) -> None:
        print(cls.cursor_show, end='', file=file)
        file.flush()
