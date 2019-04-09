"""Program ``faust reset`` used to delete local table state."""
from shutil import rmtree
from .base import AppCommand

__all__ = ['clean_versions']


class clean_versions(AppCommand):
    """Delete old version directories.

    Warning:
        This command will result in the destruction of the following files:

            1) Table data for previous versions of the app.
    """

    async def run(self) -> None:
        """Execute command."""
        self.remove_old_versiondirs()

    def remove_old_versiondirs(self) -> None:
        """Remove data from old application versions from data directory."""
        for dir in self.app.conf.find_old_versiondirs():
            self.say(f'Removing old version directory {dir}...')
            rmtree(str(dir))
