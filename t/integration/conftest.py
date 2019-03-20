import os
import subprocess
import sys
from pathlib import Path
from typing import Callable, Tuple
from faust.types import AppT
from faust.utils.json import loads
import pytest
from t.integration import app as _app_module


@pytest.fixture
def app() -> AppT:
    os.environ.pop('F_DATADIR', None)
    os.environ.pop('FAUST_DATADIR', None)
    os.environ.pop('F_WORKDIR', None)
    os.environ.pop('FAUST_WORKDIR', None)
    return _app_module.app


@pytest.fixture
def main_path() -> Path:
    return Path(_app_module.__file__).with_suffix('.py')


CommandReturns = Tuple[int, str, str]


def _create_faust_cli(executable: Path, *partial_args: str,
                      color: bool = False,
                      json: bool = False) -> Callable[..., CommandReturns]:
    if not color:
        partial_args += ('--no-color',)
    if json:
        partial_args += ('--json',)

    def call_faust_cli(*args: str) -> Tuple[str, str]:
        p = subprocess.Popen(
            [sys.executable,
             str(executable)] + list(partial_args) + list(args),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
        )
        stdout, stderr = p.communicate()
        if json:
            print(f'JSON RET: {p.returncode} {stdout!r} {stderr!r}')
            ret = p.returncode, loads(stdout), stderr
            return ret
        print(f'TEXT RET: {p.returncode} {stdout!r} {stderr!r}')
        return p.returncode, stdout, stderr
    return call_faust_cli


@pytest.fixture
def faust(main_path: Path) -> Callable[..., CommandReturns]:
    return _create_faust_cli(main_path)


@pytest.fixture
def faust_json(main_path: Path):
    return _create_faust_cli(main_path, json=True)


@pytest.fixture
def faust_color(main_path: Path) -> Callable[..., CommandReturns]:
    return _create_faust_cli(main_path, color=True)
