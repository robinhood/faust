import asyncio
import random
import sys
from typing import Any, Set
from faust.cli import option
from mode.utils.aiter import aiter, anext


def install_produce_command(app) -> None:
    @app.command(
        option('--max-latency',
               type=float, default=0.5, envvar='PRODUCE_LATENCY',
               help='Add delay of (at most) n seconds between publishing.'),
        option('--max-messages',
               type=int, default=None,
               help='Send at most N messages or 0 for infinity.'),
    )
    async def produce(self, max_latency: float, max_messages: int):
        """Produce example events."""
        prods = {aiter(p(max_messages)) for p in app.stress_producers}
        i = 0
        while not app.should_stop:
            to_remove: Set[Any] = set()
            for producer in prods:
                i += 1
                try:
                    await anext(producer)
                except StopAsyncIteration:
                    to_remove.add(producer)
                if not max_latency:
                    # no latency, print every 10,000 messages
                    if not i % 10000:
                        self.say(f'+SEND {i}')
                else:
                    # with latency, print every 10 messages
                    if not i % 10:
                        self.say(f'+SEND {i}')
            if not prods:
                await asyncio.sleep(1.0)
            if max_latency:
                await asyncio.sleep(random.uniform(0, max_latency))
            for producer in to_remove:
                prods.discard(producer)
        print('No more producers - exiting', file=sys.stderr)
