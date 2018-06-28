#!/usr/bin/env python
import random
import signal
from itertools import cycle
from typing import List, NamedTuple

import envoy
from mode import Service, Worker

# Periods describe how often we sleep between signalling workers.
# Some times we terminate them gracefully, other times we have bursts
# of abruptly terminating the process with `kill -9`.


class Period(NamedTuple):
    count: int
    signals: List[int]
    min_latency: float
    max_latency: float


periods = [
    # Signal -TERM/-INT between every 1 and 30 seconds.
    # This period lasts for at least half a minute, but never for more
    # than 50 minutes.
    Period(
        count=100,
        signals=[signal.SIGTERM, signal.SIGINT],
        min_latency=1.0,
        max_latency=30.0,
    ),
    # Signal -TERM between every 5 and 100 seconds.
    # lasts for at least 4.1 minutes, at most 83 minutes.
    Period(
        count=50,
        signals=[signal.SIGTERM],
        min_latency=5.0,
        max_latency=100.0,
    ),
    # super fast burst of signal -9 (between every 0.1s and 1.0 second).
    # lasts for at most 30 seconds.
    # This emulates what happens in production sometimes
    Period(
        count=30,
        signals=[signal.SIGKILL],
        min_latency=0.1,
        max_latency=1.0,
    ),
    # we repeat here forever, (see iter_periods()) below.)
    # --
]


def iter_periods():
    for period in cycle(periods):
        for _ in range(period.count):
            yield period


class Killer(Service):

    @Service.task
    async def _killing(self):
        print('STARTING')
        it = iter_periods()
        while not self.should_stop:
            period = next(it)
            secs = random.uniform(period.min_latency, period.max_latency)
            print(f'Sleeping for {secs} seconds...')
            await self.sleep(secs)
            sig = random.choice(period.signals)
            print(f'Signalling all workers on this box with {sig!r}')
            r = envoy.run(f'pkill -{int(sig)} Faust:Worker')
            if r.status_code:
                if r.std_err.strip():
                    print(f'ERROR from pkill: {r.std_err}')
                else:
                    print('No processes running, nothing to signal!')


if __name__ == '__main__':
    Worker(Killer(), loglevel='INFO').execute_from_commandline()
