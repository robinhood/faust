# -*- coding: utf-8 -*-
"""Python Stream processing."""
# :copyright: (c) 2017-2019, Robinhood Markets, Inc.
#             All rights reserved.
# :license:   BSD (3 Clause), see LICENSE for more details.

# -- Faust is a Python stream processing library
# mainly used with Kafka, but is generic enough to be used for general
# agent and channel based programming.

# If you are here to read the code, we suggest you start with:
#
#  faust/app.py            - Faust Application
#  faust/channels.py       - Channels for communication.
#  faust/topics.py         - A topic is a named channel (e.g. a Kafka topic)
#  faust/streams.py        - The stream iterates over events in a channel.
#  faust/tables.py         - Data is stored in tables.
#  faust/agents.py         - Agents use all of the above.
# --- ~~~~~ ~ ~  ~           ~             ~   ~                   ~
import os
import re
import sys
import typing
from typing import Any, Mapping, NamedTuple, Optional, Sequence, Tuple

__version__ = '1.5.1'
__author__ = 'Robinhood Markets, Inc.'
__contact__ = 'contact@fauststream.com'
__homepage__ = 'http://faust.readthedocs.io/'
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
_match = re.match(r'(\d+)\.(\d+).(\d+)(.+)?', __version__)
if _match is None:  # pragma: no cover
    raise RuntimeError('THIS IS A BROKEN RELEASE!')
_temp = _match.groups()
VERSION = version_info = version_info_t(
    int(_temp[0]), int(_temp[1]), int(_temp[2]), _temp[3] or '', '')
del(_match)
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
def _extract_arg_from_argv(  # pragma: no cover
        argv: Sequence[str] = sys.argv,
        *,
        shortopts: Tuple[str, ...] = (),
        longopts: Tuple[str, ...] = ('--datadir',)) -> Optional[str]:
    for i, arg in enumerate(argv):  # pragma: no cover
        if arg in shortopts:
            try:
                value = argv[i + 1]
            except IndexError:
                import click
                raise click.UsageError(f'Missing value for {arg} option')
            return value
        if arg.startswith(longopts):  # pragma: no cover
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


_datadir = (_extract_arg_from_argv(longopts=('--datadir',)) or
            os.environ.get('FAUST_DATADIR') or
            os.environ.get('F_DATADIR'))
if _datadir:  # pragma: no cover
    os.environ['FAUST_DATADIR'] = _datadir
_loop = (_extract_arg_from_argv(shortopts=('-L',), longopts=('--loop',)) or
         os.environ.get('FAUST_LOOP') or
         os.environ.get('F_LOOP'))
if _loop:  # pragma: no cover
    os.environ['FAUST_LOOP'] = _loop
    import mode.loop
    mode.loop.use(_loop)

# To ensure `import faust` executes quickly, this module imports
# attributes lazily. The next section provides static type checkers
# with information about the contents of this module.
if typing.TYPE_CHECKING:  # pragma: no cover
    from mode import Service, ServiceT                          # noqa: E402
    from .agents import Agent                                   # noqa: E402
    from .app import App                                        # noqa: E402
    from .auth import (                                         # noqa: E402
        GSSAPICredentials,
        SASLCredentials,
        SSLCredentials,
    )
    from .channels import Channel, ChannelT                     # noqa: E402
    from .events import Event, EventT                           # noqa: E402
    from .models import Model, ModelOptions, Record             # noqa: E402
    from .sensors import Monitor, Sensor                        # noqa: E402
    from .serializers import Codec                              # noqa: E402
    from .streams import Stream, StreamT, current_event         # noqa: E402
    from .tables.table import Table                             # noqa: E402
    from .tables.sets import SetTable                           # noqa: E402
    from .topics import Topic, TopicT                           # noqa: E402
    from .types.settings import Settings                        # noqa: E402
    from .windows import (                                      # noqa: E402
        HoppingWindow,
        TumblingWindow,
        SlidingWindow,
        Window,
    )
    from .worker import Worker                                # noqa: E402
    from .utils import uuid

__all__ = [
    'Agent',
    'App',
    'AppCommand',
    'Command',
    'Channel',
    'ChannelT',
    'Event',
    'EventT',
    'Model',
    'ModelOptions',
    'Record',
    'Monitor',
    'Sensor',
    'SetTable',
    'Codec',
    'Service',
    'ServiceT',
    'Stream',
    'StreamT',
    'current_event',
    'Table',
    'Topic',
    'TopicT',
    'GSSAPICredentials',
    'SASLCredentials',
    'SSLCredentials',
    'Settings',
    'HoppingWindow',
    'TumblingWindow',
    'SlidingWindow',
    'Window',
    'Worker',
    'uuid',
]

# Lazy loading.
# - See werkzeug/__init__.py for the rationale behind this.
from types import ModuleType  # noqa

all_by_module: Mapping[str, Sequence[str]] = {
    'faust.agents': ['Agent'],
    'faust.app': ['App'],
    'faust.channels': ['Channel', 'ChannelT'],
    'faust.events': ['Event', 'EventT'],
    'faust.models': ['ModelOptions', 'Record'],
    'faust.sensors': ['Monitor', 'Sensor'],
    'faust.serializers': ['Codec'],
    'faust.streams': [
        'Stream',
        'StreamT',
        'current_event',
    ],
    'faust.tables.sets': ['SetTable'],
    'faust.tables.table': ['Table'],
    'faust.topics': ['Topic', 'TopicT'],
    'faust.auth': [
        'GSSAPICredentials',
        'SASLCredentials',
        'SSLCredentials',
    ],
    'faust.types.settings': ['Settings'],
    'faust.windows': [
        'HoppingWindow',
        'TumblingWindow',
        'SlidingWindow',
        'Window',
    ],
    'faust.worker': ['Worker'],
    'faust.utils': ['uuid'],
    'mode.services': ['Service', 'ServiceT'],
}

object_origins = {}
for module, items in all_by_module.items():
    for item in items:
        object_origins[item] = module


class _module(ModuleType):
    """Customized Python module."""

    standard_package_vars = [
        '__file__',
        '__path__',
        '__doc__',
        '__all__',
        '__docformat__',
        '__name__',
        '__path__',
        'VERSION',
        'version_info_t',
        'version_info',
        '__package__',
        '__version__',
        '__author__',
        '__contact__',
        '__homepage__',
        '__docformat__',
    ]

    def __getattr__(self, name: str) -> Any:
        if name in object_origins:
            module = __import__(  # type: ignore
                object_origins[name], None, None, [name])
            for extra_name in all_by_module[module.__name__]:
                setattr(self, extra_name, getattr(module, extra_name))
            return getattr(module, name)
        return ModuleType.__getattribute__(self, name)

    def __dir__(self) -> Sequence[str]:
        return sorted(list(new_module.__all__) + self.standard_package_vars)


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
