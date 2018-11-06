#!/usr/bin/env python
import asyncio
import string
import faust

WORDS = list(string.ascii_uppercase)

app = faust.App(
    'word-counts',
    broker='kafka://localhost:9092',
    store='rocksdb://',
    version=5,
    topic_partitions=8,
)

posts_topic = app.topic('posts3', value_type=str)
word_counts = app.Table('word_counts', default=int,
                        help='Keep count of words (str to int).')


@app.agent(posts_topic)
async def shuffle_words(posts):
    async for post in posts:
        for word in post.split():
            await count_words.send(key=word, value=word)


@app.agent(value_type=str)
async def count_words(words):
    """Count words from blog post article body."""
    async for word in words:
        word_counts[word] += 1


@app.page('/count/{word}/')
@app.table_route(table=word_counts, match_info='word')
async def get_count(web, request, word):
    return web.json({
        word: word_counts[word],
    })


@app.on_rebalance_complete.connect
async def on_rebalance_complete(sender, **kwargs):
    print(word_counts.as_ansitable(
        key='word',
        value='count',
        title='$$ TALLY - after rebalance $$',
        sort=True,
    ))


@app.timer(10.0)
async def dump_count():
    print(word_counts.as_ansitable(
        key='word',
        value='count',
        title='$$ TALLY $$',
        sort=True,
    ))


@app.command()
async def produce():
    for i in range(100):
        last_fut = None
        for j in range(app.conf.topic_partitions):
            word = WORDS[j]
            for _ in range(1000):
                last_fut = await count_words.send(
                    key=word, value=word, partition=j)
        await last_fut  # wait for buffer to flush
        await asyncio.sleep(2.0)
        print(i)


if __name__ == '__main__':
    app.main()
