#!/usr/bin/env python
import os
import random
import signal
import envoy
from mode import Service, Worker

MIN = float(os.environ.get('KILLER_MIN_SECS', 5.0))
MAX = float(os.environ.get('KILLER_MAX_SECS', 6.0))


class Killer(Service):

    @Service.task
    async def _killing(self):
        print('STARTING')
        while not self.should_stop:
            secs = random.uniform(MIN, MAX)
            print(f'Sleeping for {secs} seconds...')
            await self.sleep(secs)
            sig = random.choice([signal.SIGTERM, signal.SIGINT])
            print(f'Killing all workers on this box with {sig!r}')
            r = envoy.run(f'pkill -{int(sig)} Faust:Worker')
            if r.status_code:
                if r.std_err.strip():
                    print(f'ERROR from pkill: {r.std_err}')
                else:
                    print('No processes running, nothing to kill!')


if __name__ == '__main__':
    Worker(Killer(), loglevel='INFO').execute_from_commandline()
