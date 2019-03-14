def test_command_returns_nonzero_exit_status(*, faust):
    exitcode, stdout, stderr = faust('error_command')
    assert not stdout
    assert stderr
    assert exitcode
