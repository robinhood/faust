import faust
import pytest


@pytest.fixture
def app():
    return faust.App('funtest', store='memory://')
