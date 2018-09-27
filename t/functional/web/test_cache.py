from itertools import count

import pytest
import faust
from faust.web import Blueprint, View
from faust.web.blueprints import FutureCache
from faust.web.cache import Cache

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
        app_cache = cache.resolve(self.app)
        cache_key = app_cache.key_for_request(request, None, request.method)
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


def test_aaa_blueprint():
    assert isinstance(cache, FutureCache)


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


def _cache_keys(app):
    return app.cache._data


def _expiry_timeouts(app):
    return app.cache._expires


def _time_index(app):
    return app.cache._time_index


@pytest.mark.asyncio
async def test_cached_view(*, app, bp, site, web_client, web):
    assert isinstance(cache.resolve(app), Cache)
    client = await web_client(site.web._app)
    urlA = web.url_for('test:a')
    urlB = web.url_for('test:b')
    urlC = web.url_for('test:c')
    memory = _cache_keys(app)
    expiry_timeouts = _expiry_timeouts(app)
    time_index = _time_index(app)
    assert cache.key_prefix == 'test'
    assert urlA == '/test/A/'
    responseA = await model_response(await client.get(urlA))
    keyA = responseA.key
    assert '.test.' in keyA
    assert responseA.value == 0
    assert await model_value(await client.get(urlA)) == 0
    assert keyA in memory
    assert expiry_timeouts[keyA] == DEFAULT_TIMEOUT

    assert await model_value(await client.get(urlA)) == 0

    time_index[keyA] -= DEFAULT_TIMEOUT
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
    assert expiry_timeouts[keyB] == VIEW_B_TIMEOUT
    time_index[keyB] -= VIEW_B_TIMEOUT
    assert await model_value(await client.get(urlB)) == 1
    time_index[keyB] -= VIEW_B_TIMEOUT
    assert await model_value(await client.get(urlB)) == 2
    time_index[keyB] -= VIEW_B_TIMEOUT
    assert await model_value(await client.get(urlB)) == 3

    # A is now 1
    # B is at 3
    # C should still be at 0
    assert await model_value(await client.get(urlC)) == 0
    assert await model_value(await client.get(urlC)) == 0
