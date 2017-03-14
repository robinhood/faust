import asyncio
import faust
import logging
import sys
logging.basicConfig(level=logging.INFO)


class Withdrawal(faust.Event, serializer='json'):
    amount: float


topic = faust.topic('mytopic', type=Withdrawal)


@faust.stream(topic)
async def all_withdrawals(it):
    return (event async for event in it)


async def find_large_withdrawals(withdrawals):
    async for withdrawal in withdrawals:
        if withdrawal.amount > 9999.0:
            print('ALERT: large withdrawal: {0.amount!r}'.format(withdrawal))


async def main():
    app = faust.App('aiokafka://localhost:9092')

    async def produce():
        print('----------------------NOW DO OURS-------------------')
        for i in range(100):
            await app.send(topic, None, Withdrawal(100.3 + i))
        await app.producer.stop()

    async def consume():
        withdrawals = app.add_stream(all_withdrawals)
        app.add_task(find_large_withdrawals(withdrawals))

    if sys.argv[1] == 'produce':
        await produce()
    elif sys.argv[1] == 'consume':
        await consume()
    else:
        print('Unknown command: {0!r}'.format(sys.argv[1]))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
    loop.close()
