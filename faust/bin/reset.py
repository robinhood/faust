from typing import Any
import click
from .base import AppCommand, cli

__all__ = ['reset']


@cli.command(help='Delete local table state')
@click.pass_context
class reset(AppCommand):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self()

    async def run(self) -> None:
        await self.reset_tables()
        await self.reset_checkpoints()

    async def reset_tables(self) -> None:
        for table in self.app.tables.values():
            self.say(f'Removing database for table {table.name}...')
            table.reset_state()

    async def reset_checkpoints(self) -> None:
        self.say(f'Removing file "{self.app.checkpoint_path}"...')
        self.app.checkpoints.reset_state()
