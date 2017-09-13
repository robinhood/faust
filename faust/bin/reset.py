from .base import AppCommand

__all__ = ['reset']


class reset(AppCommand):
    """Delete local table state."""

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


reset_cli = reset.as_click_command()
