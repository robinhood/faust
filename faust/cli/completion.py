import os
from pathlib import Path
import click_completion
from .base import AppCommand


class completion(AppCommand):
    """Output shell completion to be eval'd by the shell."""

    require_app = False

    async def run(self) -> None:
        self.say(click_completion.get_code(shell=self.shell()))

    def shell(self) -> str:
        shell_path = Path(os.environ.get('SHELL', 'auto'))
        return shell_path.stem
