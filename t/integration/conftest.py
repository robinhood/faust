import subprocess
from pathlib import Path
from typing import Callable, Tuple
from faust.types import AppT
from faust.utils.json import loads
import pytest
from t.integration import app as _app_module


@pytest.fixture
def app() -> AppT:
    return _app_module.app


@pytest.fixture
def main_path() -> Path:
    return Path(_app_module.__file__).with_suffix('.py')


def _create_faust_cli(executable: Path, *partial_args: str,
                      color: bool = False,
                      json: bool = False) -> Callable[..., Tuple[str, str]]:
    if not color:
        partial_args += ('--no-color',)
    if json:
        partial_args += ('--json',)

    def call_faust_cli(*args: str) -> Tuple[str, str]:
        p = subprocess.Popen(
            [str(executable)] + list(partial_args) + list(args),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
        )
        stdout, stderr = p.communicate()
        if json:
            return loads(stdout), stderr
        return stdout, stderr
    return call_faust_cli


@pytest.fixture
def faust(main_path: Path) -> Callable[..., Tuple[str, str]]:
    return _create_faust_cli(main_path)


@pytest.fixture
def faust_json(main_path: Path):
    return _create_faust_cli(main_path, json=True)


@pytest.fixture
def faust_color(main_path: Path) -> Callable[..., Tuple[str, str]]:
    return _create_faust_cli(main_path, color=True)
