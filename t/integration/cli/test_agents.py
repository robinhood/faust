def test_json(faust_json):
    exitcode, agents, stderr = faust_json('agents', '--local')
    assert not exitcode

    assert {'name': '@app.mul',
            'topic': 't-integration-app.mul',
            'help': 'Foo agent help.'} in agents
    assert {'name': '@app.add',
            'topic': 'add-topic',
            'help': '<N/A>'} in agents
    assert {'name': '@app.internal',
            'topic': '<LOCAL>',
            'help': '<N/A>'} in agents

    names = [agent['name'] for agent in agents]
    assert names.index('@app.add') < \
        names.index('@app.internal') < \
        names.index('@app.mul')


def test_tabulated(faust):
    exitcode, stdout, stderr = faust('agents', '--local')
    assert not exitcode
    assert b'@app.mul' in stdout


def test_colors(faust_color):
    exitcode, stdout, stderr = faust_color('agents', '--local')
    assert not exitcode
    assert b'@app.mul' in stdout


def test_json_no_local(faust_json):
    exitcode, agents, stderr = faust_json('agents')
    assert not exitcode

    names = [agent['name'] for agent in agents]
    assert '@app.mul' in names
    assert '@app.add' in names
    assert '@app.internal' not in names
