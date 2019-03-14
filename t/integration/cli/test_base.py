def test_command_returns_nonzero_exit_status(*, faust):
    exitcode, stdout, stderr = faust('error_command')
    print(stdout)
    print(stderr)
    assert stderr
    assert exitcode
