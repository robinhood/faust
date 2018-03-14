import faust
import pytest


@pytest.fixture
def app():
    app = faust.App('funtest', store='memory://')
    app.finalize()
    return app
