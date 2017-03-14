import asyncio
import faust

import logging
logging.basicConfig(level=logging.DEBUG)


class Withdrawal(faust.Event):
    amount: float


topic = faust.topic('mytopic', Withdrawal)


async def main():
    app = faust.App()
    #producer = app.producer
    #await producer.start()
    #await producer.send_and_wait('mytopic', value=b'foobar', key=b'cs')
    #app._producer_started = True

    print('----------------------NOW DO OURS-------------------')
    await app.send(topic, None, Withdrawal(100.3))
    await app.producer.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
