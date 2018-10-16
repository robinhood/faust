#!/usr/bin/env python
import asyncio
import faust

WORDS = ['the', 'quick', 'brown', 'fox']


app = faust.App(
    'word-counts',
    broker='kafka://localhost:9092',
    store='rocksdb://',
    version=1,
    topic_partitions=8,
)

posts_topic = app.topic('posts', value_type=str)
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


@app.task
async def sender():
    await posts_topic.maybe_declare()

    for word in WORDS:
        for _ in range(1000):
            await shuffle_words.send(value=word)

    await asyncio.sleep(5.0)
    print(word_counts.as_ansitable(
        key='word',
        value='count',
        title='$$ TALLY $$',
        sort=True,
    ))


if __name__ == '__main__':
    app.main()
