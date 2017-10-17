def test_json(faust_json):
    models, stderr = faust_json('models', '--builtins')
    assert not stderr

    assert ['Arena', '<N/A>'] in models
    assert ['Point', '<N/A>'] in models
    assert ['@RRRes', 'Request-Reply response.'] in models

    names = [model[0] for model in models]
    assert (names.index('@RRRes') <
            names.index('Arena') <
            names.index('Point'))  # sorted


def test_tabulated(faust):
    stdout, stderr = faust('models', '--builtins')
    assert not stderr
    assert b'Arena' in stdout


def test_colors(faust_color):
    stdout, stderr = faust_color('models', '--builtins')
    assert not stderr
    assert b'Point' in stdout


def test_json_no_local(faust_json):
    models, stderr = faust_json('models')
    assert not stderr

    names = [model[0] for model in models]
    assert 'Arena' in names
    assert 'Point' in names
    assert names.index('Arena') < names.index('Point')  # sorted
