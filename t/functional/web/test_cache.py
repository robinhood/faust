from itertools import count

import pytest
import faust
from faust.web import Blueprint, View

DEFAULT_TIMEOUT = 361.363
VIEW_B_TIMEOUT = 64.3

blueprint = Blueprint('test')
cache = blueprint.cache(timeout=DEFAULT_TIMEOUT)


class ResponseModel(faust.Record):
    key: str
    value: int


@blueprint.route('/A/', name='a')
class ACachedView(View):

    def __post_init__(self) -> None:
        self.counter = count()

    @cache.view()
    async def get(self, request):
        return await self._next_response(request)

    async def _next_response(self, request):
        cache_key = cache.key_for_request(request, None, request.method)
        return self.json(ResponseModel(
            key=cache_key,
            value=next(self.counter),
        ))


@blueprint.route('/B/', name='b')
class BCachedView(ACachedView):

    @cache.view(timeout=VIEW_B_TIMEOUT)
    async def get(self, request):
        return await self._next_response(request)


@blueprint.route('/C', name='c')
class CCachedView(ACachedView):
    ...


def test_cache():
    assert cache.key_prefix == 'test'


@pytest.mark.asyncio
@pytest.mark.parametrize('expected_backend', [
    pytest.param('redis', marks=pytest.mark.app(cache='redis://')),
    pytest.param('memory', marks=pytest.mark.app(cache='memory://')),
])
async def test_cached_view__redis(expected_backend, *,
                                  app, bp, web_client, web, mocked_redis):
    cache_backend = app.cache
    if cache_backend.url.scheme == 'redis':
        storage = mocked_redis.storage
        timeout_t = int
    else:
        storage = app.cache.storage
        timeout_t = lambda timeout: timeout  # noqa
    async with cache_backend:
        assert cache_backend.url.scheme == expected_backend

        client = await web_client(web._app)
        urlA = web.url_for('test:a')
        urlB = web.url_for('test:b')
        urlC = web.url_for('test:c')
        assert urlA == '/test/A/'
        responseA = await model_response(await client.get(urlA))
        keyA = responseA.key
        assert '.test.' in keyA
        assert responseA.value == 0
        assert await model_value(await client.get(urlA)) == 0
        assert storage.get(keyA)
        assert (timeout_t(storage.last_set_ttl(keyA)) ==
                timeout_t(DEFAULT_TIMEOUT))

        assert await model_value(await client.get(urlA)) == 0

        storage.expire(keyA)
        assert await model_value(await client.get(urlA)) == 1
        assert await model_value(await client.get(urlA)) == 1
        assert await model_value(await client.get(urlA)) == 1

        responseB = await model_response(await client.get(urlB))
        keyB = responseB.key
        assert '.test.' in keyB
        assert responseB.value == 0
        assert await model_value(await client.get(urlB)) == 0
        assert await model_value(await client.get(urlB)) == 0
        assert await model_value(await client.get(urlB)) == 0
        assert (timeout_t(storage.last_set_ttl(keyB)) ==
                timeout_t(VIEW_B_TIMEOUT))
        storage.expire(keyB)
        assert await model_value(await client.get(urlB)) == 1
        storage.expire(keyB)
        assert await model_value(await client.get(urlB)) == 2
        storage.expire(keyB)
        assert await model_value(await client.get(urlB)) == 3

        # A is now 1
        # B is at 3
        # C should still be at 0
        assert await model_value(await client.get(urlC)) == 0
        assert await model_value(await client.get(urlC)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('scheme,host,port,password,db,settings', [
    pytest.param('redis', 'h', 6379, None, 0, None,
                 marks=pytest.mark.app(cache='redis://h:6379')),
    pytest.param('redis', 'h', 6379, None, 0, None,
                 marks=pytest.mark.app(cache='redis://h:6379/')),
    pytest.param('redis', 'h', 6379, None, 0, None,
                 marks=pytest.mark.app(cache='redis://h:6379/0')),
    pytest.param('redis', 'h', 6379, None, 1, None,
                 marks=pytest.mark.app(cache='redis://h:6379/1')),
    pytest.param('redis', 'h', 6379, None, 999, None,
                 marks=pytest.mark.app(cache='redis://h:6379/999')),
    pytest.param('redis', 'ho', 6379, 'pw', 0, None,
                 marks=pytest.mark.app(cache='redis://:pw@ho:6379')),
    pytest.param('redis', 'ho', 6379, 'pw', 0, None,
                 marks=pytest.mark.app(cache='redis://user:pw@ho:6379')),
    pytest.param('redis', 'h', 6379, None, 1, {'max_connections': 10},
                 marks=pytest.mark.app(
                     cache='redis://h:6379/1?max_connections=10')),
    pytest.param('redis', 'h', 6, None, 0,
                 {'max_connections': 10, 'stream_timeout': 8},
                 marks=pytest.mark.app(
                     cache='redis://h:6?max_connections=10&stream_timeout=8')),
])
async def test_cache__redis_url(scheme, host, port, password, db, settings,
                                *, app, mocked_redis):
    settings = dict(settings or {})
    settings.setdefault('connect_timeout', None)
    settings.setdefault('stream_timeout', None)
    settings.setdefault('max_connections', None)
    settings.setdefault('max_connections_per_node', None)
    await app.cache.connect()
    mocked_redis.assert_called_once_with(
        host=host,
        port=port,
        password=password,
        db=db,
        skip_full_coverage_check=True,
        **settings)


async def model_response(response, *,
                         expected_status: int = 200,
                         expected_content_type: str = 'application/json',
                         model=ResponseModel):
    assert response.status == 200
    assert response.content_type == expected_content_type
    return model.from_data(await response.json())


async def model_value(response, **kwargs):
    return (await model_response(response, **kwargs)).value


@pytest.fixture()
def bp(app):
    blueprint.register(app, url_prefix='/test/')
