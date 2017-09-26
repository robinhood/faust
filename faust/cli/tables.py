"""Program ``faust tables`` used to list tables.

.. program:: faust tables
"""
from .base import AppCommand

DEFAULT_TABLE_HELP = 'Missing description: use Table(.., help="str")'


class tables(AppCommand):
    """List tables."""

    async def run(self) -> None:
        self.say(self.tabulate([
            (t.name, t.help or DEFAULT_TABLE_HELP)
            for t in self.app.tables.values()
        ], title='Tables'))
