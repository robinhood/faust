def test_json(faust_json):
    agents, stderr = faust_json('agents', '--local')
    assert not stderr

    assert ['@mul', 'app.mul', 'Foo agent help.'] in agents
    assert ['@add', 'add-topic', '<N/A>'] in agents
    assert ['@internal', '<LOCAL>', '<N/A>'] in agents

    names = [agent[0] for agent in agents]
    assert names.index('@add') < names.index('@internal') < names.index('@mul')


def test_tabulated(faust):
    stdout, stderr = faust('agents', '--local')
    assert not stderr
    assert b'@mul' in stdout


def test_colors(faust_color):
    stdout, stderr = faust_color('agents', '--local')
    assert not stderr
    assert b'@mul' in stdout


def test_json_no_local(faust_json):
    agents, stderr = faust_json('agents')
    assert not stderr

    names = [agent[0] for agent in agents]
    assert '@mul' in names
    assert '@add' in names
    assert '@internal' not in names
