import os
import pytest
from faust.cli._env import _getenv


def test_getenv_not_set():
    os.environ.pop('THIS_IS_NOT_SET', None)
    with pytest.raises(KeyError):
        _getenv('THIS_IS_NOT_SET')
