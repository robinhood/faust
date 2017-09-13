from .base import AppCommand


class actors(AppCommand):
    """List actors."""

    async def run(self) -> None:
        self.say(self.tabulate(
            [[self._name(k)] for k in self.app.actors],
            headers=['name'],
        ))

    def _name(self, name: str) -> str:
        if name.startswith(self.app.origin):
            name = name[len(self.app.origin) + 1:]
        return f'@{name}'
