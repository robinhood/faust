import asyncio
import io
import os
import sys
from contextlib import ExitStack, redirect_stderr, redirect_stdout
import pytest
from mode.utils.mocks import patch


def test_main(*, app, loop):
    from proj324 import main

    neu_loop = asyncio.set_event_loop(asyncio.new_event_loop())  # noqa

    # must use the event loop fixture to ensure not using old loop.
    stdout = io.StringIO()
    stderr = io.StringIO()
    environ = dict(os.environ)
    try:
        with ExitStack() as stack:
            stack.enter_context(patch(
                'sys.argv',
                ['proj', 'my_process_command_i324']))
            assert sys.argv == ['proj', 'my_process_command_i324']
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
    assert 'HELLO WORLD #324' in stdout.getvalue()
