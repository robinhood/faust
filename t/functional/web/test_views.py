import faust
import pytest
from faust import web

blueprint = web.Blueprint('test_views')


class XModel(faust.Record):
    aint: int
    astr: str


@blueprint.route('/takes/')
class TakeView(web.View):

    @web.takes_model(XModel)
    @web.gives_model(XModel)
    async def post(self, request, obj):
        return obj


@blueprint.route('/gives/')
class GivesView(web.View):

    @web.gives_model(XModel)
    async def get(self, request):
        return XModel(11, 'world')


@blueprint.route('/options/')
class OptionsView(web.View):

    async def options(self, request):
        return self.json(
            None,
            headers={'Access-Control-Allow-Methods': 'GET, OPTIONS'},
        )


@pytest.fixture()
def inject_blueprint(app):
    app.web.blueprints.add('/test/', blueprint)


@pytest.mark.asyncio
async def test_takes_model(*, inject_blueprint, web_client, app):
    client = await web_client
    obj = XModel(aint=30, astr='hello')
    resp = await client.post('/test/takes/', data=obj.dumps(serializer='json'))
    assert resp.status == 200
    payload = await resp.json()
    assert XModel.from_data(payload) == obj


@pytest.mark.asyncio
async def test_gives_model(*, inject_blueprint, web_client, app):
    client = await web_client
    resp = await client.get('/test/gives/')
    assert resp.status == 200
    payload = await resp.json()
    assert XModel.from_data(payload) == XModel(11, 'world')


@pytest.mark.asyncio
async def test_options(*, inject_blueprint, web_client, app):
    client = await web_client
    resp = await client.options('/test/options/')
    assert resp.status == 200
    assert resp.headers['Access-Control-Allow-Methods'] == 'GET, OPTIONS'
