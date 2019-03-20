from itertools import count

import aredis
import pytest
import faust
from faust.exceptions import ImproperlyConfigured
from faust.web import Blueprint, View
from faust.web.cache import backends
from faust.web.cache.backends import redis
from mode.utils.mocks import Mock

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
@pytest.mark.app(cache='memory://')
async def test_cached_view__HEAD(*, app, bp, web_client, web):
    app.cache.storage.clear()
    async with app.cache:
        client = await web_client
        urlA = web.url_for('test:a')
        response = await client.head(urlA)
        assert response.status == 200
        response2 = await client.head(urlA)
        assert response2.status == 200


@pytest.mark.asyncio
@pytest.mark.app(cache='memory://')
async def test_cached_view__cannot_cache(*, app, bp, web_client, web):
    prev = cache.can_cache_request
    cache.can_cache_request = Mock(name='can_cache_request')
    cache.can_cache_request.return_value = False
    try:
        app.cache.storage.clear()
        async with app.cache:
            client = await web_client
            urlA = web.url_for('test:a')
            response = await client.get(urlA)
            assert response.status == 200
            response2 = await client.get(urlA)
            assert response2.status == 200
    finally:
        cache.can_cache_request = prev


@pytest.mark.app(cache='memory://')
def test_key_for_request(*, app):
    _cache = blueprint.cache(timeout=DEFAULT_TIMEOUT)
    request = Mock(name='request')
    _cache.build_key = Mock(name='build_key')
    _cache.key_for_request(request, prefix='/foo/')
    _cache.build_key.assert_called_once_with(
        request,
        request.method,
        '/foo/',
        [],
    )


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

        client = await web_client
        urlA = web.url_for('test:a')
        urlB = web.url_for('test:b')
        urlC = web.url_for('test:c')
        assert urlA == '/test/A/'
        assert storage.ttl('does-not-exist') is None
        responseA = await model_response(await client.get(urlA))
        keyA = responseA.key
        assert '.test.' in keyA
        assert responseA.value == 0
        assert await model_value(await client.get(urlA)) == 0
        assert storage.get(keyA)
        assert storage.ttl(keyA)
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
async def test_redis__url(scheme, host, port, password, db, settings,
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


@pytest.mark.asyncio
@pytest.mark.app(cache='redis://h:6079//')
async def test_redis__url_invalid_path(app, mocked_redis):
    with pytest.raises(ValueError):
        await app.cache.connect()


@pytest.mark.asyncio
@pytest.mark.app(cache='redis://h:6079//')
async def test_redis__using_starting_(app, mocked_redis):
    with pytest.raises(RuntimeError):
        app.cache.client


@pytest.fixture()
def no_aredis(monkeypatch):
    monkeypatch.setattr('faust.web.cache.backends.redis.aredis', None)


@pytest.mark.asyncio
@pytest.mark.app(cache='redis://')
async def test_redis__aredis_is_not_installed(*, app, no_aredis):
    cache = app.cache
    with pytest.raises(ImproperlyConfigured):
        async with cache:
            ...


@pytest.mark.asyncio
@pytest.mark.app(cache='redis://')
async def test_redis__start_twice_same_client(*, app, mocked_redis):
    async with app.cache:
        client1 = app.cache._client
        assert client1 is not None
        client1.ping.assert_called_once_with()
        await app.cache.restart()
        assert app.cache._client is client1


@pytest.mark.asyncio
@pytest.mark.app(cache='redis://')
async def test_redis_get__irrecoverable_errors(*, app, mocked_redis):
    from aredis.exceptions import AuthenticationError
    mocked_redis.return_value.get.coro.side_effect = AuthenticationError()

    with pytest.raises(app.cache.Unavailable):
        async with app.cache:
            await app.cache.get('key')


@pytest.mark.asyncio
@pytest.mark.app(cache='redis://')
@pytest.mark.parametrize('operation,delete_error', [
    ('get', False),
    ('get', True),
    ('delete', False),
    ('delete', True),
])
async def test_redis_invalidating_error(operation, delete_error, *,
                                        app, mocked_redis):
    from aredis.exceptions import DataError
    mocked_op = getattr(mocked_redis.return_value, operation)
    mocked_op.coro.side_effect = DataError()
    if delete_error:
        # then the delete fails
        mocked_redis.return_value.delete.coro.side_effect = DataError()

    with pytest.raises(app.cache.Unavailable):
        async with app.cache:
            await getattr(app.cache, operation)('key')
    if operation == 'delete':
        mocked_redis.return_value.delete.assert_called_with('key')
        assert mocked_redis.return_value.delete.call_count == 2
    else:
        mocked_redis.return_value.delete.assert_called_once_with('key')


@pytest.mark.asyncio
@pytest.mark.app(cache='memory://')
async def test_memory_delete(*, app):
    app.cache.storage.set('foo', b'bar')
    async with app.cache:
        assert await app.cache.get('foo') == b'bar'
        await app.cache.delete('foo')
        assert await app.cache.get('foo') is None


@pytest.mark.asyncio
@pytest.mark.app(cache='redis://')
async def test_redis_get__operational_error(*, app, mocked_redis):
    from aredis.exceptions import TimeoutError
    mocked_redis.return_value.get.coro.side_effect = TimeoutError()

    with pytest.raises(app.cache.Unavailable):
        async with app.cache:
            await app.cache.get('key')


def test_cache_repr(*, app):
    assert repr(app.cache)


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


class test_RedisScheme:

    def test_single_client(self, app):
        url = 'redis://123.123.123.123:3636//1'
        Backend = backends.by_url(url)
        assert Backend is redis.CacheBackend
        backend = Backend(app, url=url)
        assert isinstance(backend, redis.CacheBackend)
        client = backend._new_client()
        assert isinstance(client, aredis.StrictRedis)
        pool = client.connection_pool
        assert pool.connection_kwargs['host'] == backend.url.host
        assert pool.connection_kwargs['port'] == backend.url.port
        assert pool.connection_kwargs['db'] == 1

    def test_cluster_client(self, app):
        url = 'rediscluster://123.123.123.123:3636//1'
        Backend = backends.by_url(url)
        assert Backend is redis.CacheBackend
        backend = Backend(app, url=url)
        assert isinstance(backend, redis.CacheBackend)
        client = backend._new_client()
        assert isinstance(client, aredis.StrictRedisCluster)
        pool = client.connection_pool
        assert {'host': backend.url.host,
                'port': 3636} in pool.nodes.startup_nodes
