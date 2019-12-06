"""Program ``faust worker`` used to start application from console."""
from typing import Any
from .worker import worker

__all__ = ['livecheck']


class livecheck(worker):
    """Manage LiveCheck instances."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        try:
            livecheck = self.app.livecheck  # type: ignore
        except AttributeError:
            raise self.UsageError(
                f'App {self.app!r} is not associated with LiveCheck')
        self.app = livecheck
        self._finalize_concrete_app(self.app)
