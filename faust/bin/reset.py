from .base import AppCommand

__all__ = ['reset']


class reset(AppCommand):
    """Delete local table state.

    Warning:
        This command will result in the destruction of the following files:

            1) The ``.checkpoint`` file (or :attr:`@checkpoint_path`),
               that keeps track of for each table changelog topic,
               the offset of the last message we produced.

            2) The local database directories/files backing tables
               (does not apply if an in-memory store like memory:// is used).

    Notes:
        This data is technically recoverable from the Kafka cluster (if
        intact), but it'll take a long time to get the data back as
        you need to consume each changelog topic in total.

        It'd be faster to copy the data from any standbys that happen
        to have the topic partitions you require.
    """

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
