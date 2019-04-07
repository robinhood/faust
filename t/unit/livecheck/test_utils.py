import json
import faust
from faust.livecheck.utils import to_model


def test_to_model():

    class X(faust.Record):
        x: int
        y: int

    assert to_model('foo') == 'foo'
    assert to_model(1) == 1
    assert to_model(1.01) == 1.01

    x1 = X(10, 20)
    assert to_model(json.loads(x1.dumps(serializer='json'))) == x1
