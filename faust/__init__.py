# -*- coding: utf-8 -*-
"""Python Stream processing."""
# :copyright: (c) 2017, Robinhood Markets
#             All rights reserved.
# :license:   BSD (3 Clause), see LICENSE for more details.

# -- Faust is a Python stream processing library
# mainly used with Kafka, but is generic enough to be used for general
# agent and channel based programming.

# If you are here to read the code, we suggest you start with:
#
#  faust/app.py            - Configures the Faust instance.
#  faust/channels.py       - Agents and streams receive messages on channels.
#  faust/topics.py         - A topic is a named channel (e.g. a Kafka topic)
#  faust/streams/stream.py - The stream iterates over events in a channel.
#  faust/tables.py         - Data is stored in tables.
#  faust/agents.py         - Agents use all of the above.
# --- ~~~~~ ~ ~  ~           ~             ~   ~                   ~
import os
import re
import sys
import typing
from typing import Any, Mapping, NamedTuple, Optional, Sequence, Tuple

__version__ = '0.9.21'
__author__ = 'Robinhood Markets'
__contact__ = 'opensource@robinhood.com'
__homepage__ = 'http://fauststream.com'
__docformat__ = 'restructuredtext'

# -eof meta-


class version_info_t(NamedTuple):
    major: int
    minor: int
    micro: int
    releaselevel: str
    serial: str


# bumpversion can only search for {current_version}
# so we have to parse the version here.
_temp = re.match(
    r'(\d+)\.(\d+).(\d+)(.+)?', __version__).groups()
VERSION = version_info = version_info_t(
    int(_temp[0]), int(_temp[1]), int(_temp[2]), _temp[3] or '', '')
del(_temp)
del(re)


# This is here to support setting the --datadir argument
# when executing a faust application module directly:
#    $ python examples/word_count.py
#
# When using the ``faust`` command, the entry point is controlled by
# .cli.base.cli.  It sets the F_DATADIR environment variable before the
# app is imported, which means that when the faust.app module is imported
# the App will have the correct default paths.
#
# However, when examples/word_count.py is executed the app is instantiated
# before the CLI, so we have to use this hack to to set the
# environment variable as early as possible.
#
# A side effect is that anything importing faust
# that also happens to have a '-D' argument in sys.argv will have the
# F_DATADIR environment variable set.  I think that's ok. [ask]
def _extract_arg_from_argv(
        argv: Sequence[str] = sys.argv,
        *,
        shortopts: Tuple[str, ...] = (),
        longopts: Tuple[str, ...] = ('--datadir',)) -> Optional[str]:
    for i, arg in enumerate(argv):
        if arg in shortopts:
            try:
                value = argv[i + 1]
            except IndexError:
                import click
                raise click.UsageError(f'Missing value for {arg} option')
            return value
        if arg.startswith(longopts):
            key, _, value = arg.partition('=')
            if not value:
                try:
                    value = argv[i + 1]
                except IndexError:
                    import click
                    raise click.UsageError(
                        f'Missing {key}! Did you mean --{key}=value?')
            return value
    return None


_datadir = (
    _extract_arg_from_argv(longopts=('--datadir',)) or
    os.environ.get('FAUST_DATADIR') or
    os.environ.get('F_DATADIR')
)
if _datadir:
    os.environ['FAUST_DATADIR'] = _datadir
_loop = (
    _extract_arg_from_argv(shortopts=('-L',), longopts=('--loop',)) or
    os.environ.get('FAUST_LOOP') or
    os.environ.get('F_LOOP')
)
if _loop:
    os.environ['FAUST_LOOP'] = _loop
    import mode.loop
    mode.loop.use(_loop)

# This module loads attributes lazily so that `import faust` loads
# quickly.  The next section provides static type checkers
# information about the contents of this module.
if typing.TYPE_CHECKING:
    from mode import Service, ServiceT                          # noqa: E402
    from .agents import Agent                                   # noqa: E402
    from .app import App                                        # noqa: E402
    from .channels import Channel, ChannelT, Event, EventT      # noqa: E402
    from .models import Model, ModelOptions, Record             # noqa: E402
    from .sensors import Monitor, Sensor                        # noqa: E402
    from .serializers import Codec                              # noqa: E402
    from .streams.stream import Stream, StreamT, current_event  # noqa: E402
    from .tables.set import Set                                 # noqa: E402
    from .tables.table import Table                             # noqa: E402
    from .topics import Topic                                   # noqa: E402
    from .windows import (                                      # noqa: E402
        HoppingWindow, TumblingWindow, SlidingWindow,
    )
    from .worker import Worker                                # noqa: E402

__all__ = [
    'Agent',
    'App',
    'AppCommand', 'Command',
    'Channel',
    'ChannelT',
    'Event',
    'EventT',
    'Model', 'ModelOptions', 'Record',
    'Monitor',
    'Sensor',
    'Codec',
    'Service', 'ServiceT',
    'Stream', 'StreamT', 'current_event',
    'Set',
    'Table',
    'Topic',
    'TopicT',
    'HoppingWindow', 'TumblingWindow', 'SlidingWindow',
    'Worker',
]


# Lazy loading.
# - See werkzeug/__init__.py for the rationale behind this.
from types import ModuleType  # noqa

all_by_module: Mapping[str, Sequence[str]] = {
    'faust.agents': ['Agent'],
    'faust.app': ['App'],
    'faust.channels': ['Channel', 'ChannelT', 'Event', 'EventT'],
    'faust.models': ['ModelOptions', 'Record'],
    'faust.sensors': ['Monitor', 'Sensor'],
    'faust.serializers': ['Codec'],
    'faust.streams.stream': ['Stream', 'StreamT', 'current_event'],
    'faust.tables.set': ['Set'],
    'faust.tables.table': ['Table'],
    'faust.topics': ['Topic'],
    'faust.windows': [
        'HoppingWindow',
        'TumblingWindow',
        'SlidingWindow',
    ],
    'faust.worker': ['Worker'],
    'mode.services': ['Service', 'ServiceT'],
}

object_origins = {}
for module, items in all_by_module.items():
    for item in items:
        object_origins[item] = module


class _module(ModuleType):
    """Customized Python module."""

    def __getattr__(self, name: str) -> Any:
        if name in object_origins:
            module = __import__(
                object_origins[name], None, None, [name])
            for extra_name in all_by_module[module.__name__]:
                setattr(self, extra_name, getattr(module, extra_name))
            return getattr(module, name)
        return ModuleType.__getattribute__(self, name)

    def __dir__(self) -> Sequence[str]:
        result = list(new_module.__all__)
        result.extend(('__file__', '__path__', '__doc__', '__all__',
                       '__docformat__', '__name__', '__path__',
                       'VERSION', 'version_info_t', 'version_info',
                       '__package__', '__version__', '__author__',
                       '__contact__', '__homepage__', '__docformat__'))
        return result


# keep a reference to this module so that it's not garbage collected
old_module = sys.modules[__name__]

new_module = sys.modules[__name__] = _module(__name__)
new_module.__dict__.update({
    '__file__': __file__,
    '__path__': __path__,  # type: ignore
    '__doc__': __doc__,
    '__all__': tuple(object_origins),
    '__version__': __version__,
    '__author__': __author__,
    '__contact__': __contact__,
    '__homepage__': __homepage__,
    '__docformat__': __docformat__,
    '__package__': __package__,
    'version_info_t': version_info_t,
    'version_info': version_info,
    'VERSION': VERSION,
})
