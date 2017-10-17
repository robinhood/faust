#!/usr/bin/env python3
from typing import List
import faust


class Point(faust.Record):
    x: int
    y: int


class Arena(faust.Record):
    points: List[Point]
    timestamp: float = None


app = faust.App('t-integration', origin='t.integration.app')
add_topic = app.topic('add-topic')
local_channel = app.channel()


@app.agent()
async def mul(stream):
    """Foo agent help."""
    async for event in stream:
        yield event * 2


@app.agent(add_topic)
async def add(stream):
    async for event in stream:
        yield event + 2


@app.agent(local_channel)
async def internal(stream):
    async for event in stream:
        yield event / 2


if __name__ == '__main__':
    app.main()
