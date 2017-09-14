"""Program ``faust`` (umbrella command).

.. program:: faust

.. cmdoption:: -A, --app

    Path of Faust application to use, or the name of a module.

.. cmdoption:: --quiet, --no-quiet, -q

    Silence output to <stdout>/<stderr>.

.. cmdoption:: --debug, --no-debug

    Enable debugging output, and the blocking detector.

.. cmdoption:: --workdir

    Working directory to change to after start.

.. cmdoption:: --json, --no-json

    Prefer data to be emitted in json format.
"""

# Note: The command options above are defined in bin.base.builtin_options
from .actors import actors
from .base import cli
from .reset import reset
from .send import send
from .tables import tables
from .worker import worker

__all__ = ['actors', 'cli', 'reset', 'send', 'tables', 'worker']
