import datetime
import logging
import re
import socket
import sys
import uuid
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, IO, Iterator, NamedTuple, Set
import aiohttp
import faust
import faust.transport.utils
import mode
from yarl import URL


SETTINGS: Path = Path('docs/userguide/settings.rst')

app = faust.App('verify_defaults')

ignore_settings: Set[str] = {
    'id',
    'tabledir',
    'reply_to',
    'broker_consumer',
    'broker_producer',
}

builtin_locals: Dict[str, Any] = {
    'aiohttp': aiohttp,
    'app': app,
    'datetime': datetime,
    'datadir': app.conf.datadir,
    'faust': faust,
    'logging': logging,
    'mode': mode,
    'socket': socket,
    'timedelta': timedelta,
    'web_host': socket.gethostname(),
    'web_port': 6066,
    'VERSION': faust.__version__,
    'uuid': uuid,
    'URL': URL,
}

RE_REF = re.compile(r'^:(\w+):`')


class Error(NamedTuple):
    reason: str
    setting: str
    default: Any
    actual: Any


def verify_settings(rst_path: Path) -> Iterator[Error]:
    for setting_name, default in find_settings_in_rst(rst_path):
        actual = getattr(app.conf, setting_name)
        if isinstance(default, timedelta):
            default = default.total_seconds()
        if isinstance(actual, Enum):
            actual = actual.value
        if actual != default:
            yield Error(
                reason='mismatch',
                setting=setting_name,
                default=default,
                actual=actual,
            )


def report_errors(errors: Iterator[Error]) -> int:
    num_errors: int = 0
    for num_errors, e in enumerate(errors, start=1):
        if num_errors == 1:
            carp(f'{sys.argv[0]}: Errors in docs/userguide/settings.rst:')
        carp(f'  + Setting {e.reason} {e.setting}:')
        carp(f'       documentation: {e.default!r}')
        carp(f'              actual: {e.actual!r}')
    if num_errors:
        carp(f'Found {num_errors} error(s).', file=sys.stderr)
    else:
        print(f'{sys.argv[0]}: All OK :-)', file=sys.stdout)
    return num_errors


def carp(msg, *, file: IO = sys.stderr, **kwargs: Any) -> None:
    print(msg, file=file, **kwargs)


def find_settings_in_rst(rst_path: Path,
                         locals: Dict[str, Any] = None,
                         builtin_locals: Dict[str, Any] = builtin_locals,
                         ignore_settings: Set[str] = ignore_settings):
    setting: str = None
    default: Any = None
    app = faust.App('_verify_doc_defaults')
    _globals = dict(globals())
    # Add setting default to globals
    # so that defaults referencing another setting work.
    # E.g.:
    #   :default: :setting:`broker_api_version`
    _globals.update({
        name: getattr(app.conf, name)
        for name in app.conf.setting_names()
    })
    local_ns: Dict[str, Any] = {**builtin_locals, **(locals or {})}
    for line in rst_path.read_text().splitlines():
        if line.startswith('.. setting::'):
            if setting and not default and setting not in ignore_settings:
                raise Exception(f'No default value for {setting}')
            setting = line.split('::')[-1].strip()
        elif ':default:' in line:
            if '``' in line:
                line, sep, rest = line.rpartition('``')
            default = line.split(':default:')[-1].strip()
            default = default.strip('`')
            default = RE_REF.sub('', default)
            default_value = eval(default, _globals, local_ns)
            if setting not in ignore_settings:
                yield setting, default_value


if __name__ == '__main__':
    sys.exit(report_errors(verify_settings(SETTINGS)))
