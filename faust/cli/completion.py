"""completion - Command line utility for completion (bash, ksh, zsh, etc.)."""
import os
from pathlib import Path
from .base import AppCommand

try:
    import click_completion
except ImportError:  # pragma: no cover
    click_completion = None  # noqa
else:  # pragma: no cover
    click_completion.init()


class completion(AppCommand):
    """Output shell completion to be eval'd by the shell."""

    require_app = False

    async def run(self) -> None:
        if click_completion is None:
            raise self.UsageError(
                'Missing required dependency, but this is easy to fix.\n'
                'Run `pip install click_completion` from your virtualenv\n'
                'and try again!')
        self.say(click_completion.get_code(shell=self.shell()))

    def shell(self) -> str:
        shell_path = Path(os.environ.get('SHELL', 'auto'))
        return shell_path.stem
