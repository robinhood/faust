import sys
import faust
sys.path.insert(0, '.')
from t.bench.base import Benchmark  # noqa

app = faust.App('bench-forward')
topic = app.topic(
    'bench-forward-1st',
    value_serializer='raw',
    value_type=int,
)
consume_topic = app.topic(
    'bench-forward-2nd',
    value_serializer='raw',
    value_type=int,
)

# Producer sends to topic1 (1st)
# Agent forwarder consumes from topic1 and forwards to topic2
# Benchmark agent consumes from topic2
#
#
# So we are measuring the time it takes to go through an
# additional agent.


@app.agent(topic)
async def forwarder(stream):
    i = 0
    async for event in stream.events():
        if i and not i % 10_000:
            print(f'Forwarded: {i}')
        await event.forward(consume_topic)
        i += 1


Benchmark(app, topic, consume_topic=consume_topic).install(__name__)
