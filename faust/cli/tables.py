"""Program ``faust tables`` used to list tables."""
from .base import AppCommand

DEFAULT_TABLE_HELP = 'Missing description: use Table(.., help="str")'


class tables(AppCommand):
    """List available tables."""

    title = 'Tables'

    async def run(self) -> None:
        self.say(
            self.tabulate(
                [(self.bold(table.name),
                  self.dark(table.help or DEFAULT_TABLE_HELP))
                 for table in self.app.tables.values()],
                title=self.title,
                headers=['name', 'help']))
