#!/usr/bin/env python
import faust
TOPIC = 'test'

# host.docker.internal is how a docker container connects to the local machine.
# Don't use in production, this only works with Docker for Mac in development
app = faust.App('scores', broker='kafka://host.docker.internal:9092')
table = app.Table('totals', default=int)


class Score(faust.Record, serializer='json'):
    index: int
    value: int


test_topic = app.topic(TOPIC, value_type=Score)


def get_score_key(score):
    return f'partition: {score.index % 2}'


@app.agent(test_topic)
async def print_totals(stream):
    async for score in stream.group_by(get_score_key, name='index_partition'):
        ind = f'partition: {score.index % 2}'
        table['totals'] += 1
        table[ind] += 1
        print(f'Total: {table["totals"]}, Partition {ind}: {table[ind]}')


if __name__ == '__main__':
    app.main()
