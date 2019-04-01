import asyncio
import io
import os
import sys
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from pathlib import Path
import pytest
from mode.utils.mocks import patch

sys.path.append(str(Path(__file__).parent))

from proj import main  # noqa


def test_main(loop):
    neu_loop = asyncio.set_event_loop(asyncio.new_event_loop())  # noqa

    # must use the event loop fixture to ensure not using old loop.
    stdout = io.StringIO()
    stderr = io.StringIO()
    environ = dict(os.environ)
    try:
        with ExitStack() as stack:
            stack.enter_context(patch('sys.argv', ['proj', 'foo']))
            assert sys.argv == ['proj', 'foo']
            stack.enter_context(pytest.raises(SystemExit))
            stack.enter_context(redirect_stdout(stdout))
            stack.enter_context(redirect_stderr(stderr))

            main()
    finally:
        os.environ.clear()
        os.environ.update(environ)
    print(f'STDOUT: {stdout.getvalue()!r}')
    print(f'STDERR: {stderr.getvalue()!r}')
    assert 'IMPORTS __MAIN__' not in stdout.getvalue()
    assert 'TEST FILE IMPORTED' not in stdout.getvalue()
    assert 'HELLO WORLD' in stdout.getvalue()
