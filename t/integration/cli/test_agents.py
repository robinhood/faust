def test_json(faust_json):
    agents, stderr = faust_json('agents', '--local')
    assert not stderr

    assert ['@mul', 'app.mul', 'Foo agent help.'] in agents
    assert ['@add', 'add-topic', '<N/A>'] in agents
    assert ['@internal', '<LOCAL>', '<N/A>'] in agents

    names = [agent[0] for agent in agents]
    assert names.index('@add') < names.index('@internal') < names.index('@mul')


def test_tabulated(faust):
    exitcode, stdout, stderr = faust('agents', '--local')
    assert not exitcode
    assert not stderr
    assert b'@mul' in stdout


def test_colors(faust_color):
    exitcode, stdout, stderr = faust_color('agents', '--local')
    assert not exitcode
    assert not stderr
    assert b'@mul' in stdout


def test_json_no_local(faust_json):
    exitcode, agents, stderr = faust_json('agents')
    assert not exitcode
    assert not stderr

    names = [agent[0] for agent in agents]
    assert '@mul' in names
    assert '@add' in names
    assert '@internal' not in names
