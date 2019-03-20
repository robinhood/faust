def test_json(faust_json):
    exitcode, models, stderr = faust_json('models', '--builtins')
    assert not exitcode

    assert {'name': 'app.Arena', 'help': '<N/A>'} in models
    assert {'name': 'app.Point', 'help': '<N/A>'} in models
    assert {'name': '@ReqRepResponse',
            'help': 'Request-Reply response.'} in models

    names = [model['name'] for model in models]
    assert (names.index('@ReqRepResponse') <
            names.index('app.Arena') <
            names.index('app.Point'))  # sorted


def test_tabulated(faust):
    exitcode, stdout, stderr = faust('models', '--builtins')
    assert not exitcode
    assert b'Arena' in stdout


def test_colors(faust_color):
    exitcode, stdout, stderr = faust_color('models', '--builtins')
    assert not exitcode
    assert b'Point' in stdout


def test_json_no_local(faust_json):
    exitcode, models, stderr = faust_json('models')
    assert not exitcode

    names = [model['name'] for model in models]
    assert 'app.Arena' in names
    assert 'app.Point' in names
    assert names.index('app.Arena') < names.index('app.Point')  # sorted
