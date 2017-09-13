from .base import AppCommand


class tables(AppCommand):
    """List tables."""

    async def run(self) -> None:
        self.say(self.tabulate([[k] for k in self.app.tables],
                               headers=['name']))
