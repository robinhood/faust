import sys
import faust
sys.path.insert(0, '.')
from t.bench.base import Benchmark  # noqa

app = faust.App('bench-baseline')
topic = app.topic('bench-baseline', value_serializer='raw', value_type=int)
Benchmark(app, topic).install(__name__)
