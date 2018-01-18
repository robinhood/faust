"""Program ``faust`` (umbrella command).

.. program:: faust

.. cmdoption:: -A, --app

    Path of Faust application to use, or the name of a module.

.. cmdoption:: --quiet, --no-quiet, -q

    Silence output to <stdout>/<stderr>.

.. cmdoption:: --debug, --no-debug

    Enable debugging output, and the blocking detector.

.. cmdoption:: --workdir, -W

    Working directory to change to after start.

.. cmdoption:: --datadir

    Directory to keep application state.

.. cmdoption:: --json, --no-json

    Prefer data to be emitted in json format.

.. cmdoption:: --loop, -L

    Event loop implementation to use: aio (default), gevent, uvloop.
"""

# Note: The command options above are defined in .cli.base.builtin_options
from .agents import agents
from .base import cli
from .model import model
from .models import models
from .reset import reset
from .send import send
from .tables import tables
from .worker import worker

__all__ = [
    'agents',
    'cli',
    'model',
    'models',
    'reset',
    'send',
    'tables',
    'worker',
]
