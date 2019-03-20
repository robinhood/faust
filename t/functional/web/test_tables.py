import pytest


@pytest.fixture()
def table_foo(app):
    return app.Table('foo-table', help='First table.')


@pytest.fixture()
def table_bar(app):
    return app.Table('bar-table', help='Second table.')


@pytest.fixture()
def tables(table_foo, table_bar):
    return [table_foo, table_bar]


async def test_list_tables(web_client, tables):
    async with await web_client as client:
        resp = await client.get('/table/')
        assert resp.status == 200
        payload = await resp.json()
        tables = {t['name']: t for t in payload}
        assert 'foo-table' in tables
        assert tables['foo-table']['help'] == 'First table.'
        assert 'bar-table' in tables
        assert tables['bar-table']['help'] == 'Second table.'


async def test_table_detail(web_client, tables):
    async with await web_client as client:
        resp = await client.get('/table/foo-table/')
        assert resp.status == 200
        payload = await resp.json()
        assert payload['name'] == 'foo-table'
        assert payload['help'] == 'First table.'


async def test_table_detail__missing_table(web_client, tables):
    async with await web_client as client:
        resp = await client.get('/table/XUZZY-table/')
        assert resp.status == 404
        payload = await resp.json()
        assert payload == {
            'error': 'unknown table',
            'name': 'XUZZY-table',
        }


async def test_table_key(web_client, tables, table_foo, router_same):
    async with await web_client as client:
        table_foo.data.data['KEY'] = '303'
        resp = await client.get('/table/foo-table/KEY/')
        assert resp.status == 200
        payload = await resp.json()
        assert payload == '303'


async def test_table_key__missing_key(web_client,
                                      tables,
                                      table_foo,
                                      router_same):
    async with await web_client as client:
        resp = await client.get('/table/foo-table/MISSINGKEY/')
        assert resp.status == 404
        payload = await resp.json()
        assert payload == {
            'error': 'key not found',
            'table': 'foo-table',
            'key': 'MISSINGKEY',
        }
