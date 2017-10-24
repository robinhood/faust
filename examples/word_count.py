#!/usr/bin/env python
import asyncio
#import aioeventlet
#asyncio.set_event_loop_policy(aioeventlet.EventLoopPolicy())
#import eventlet
import faust
#eventlet.monkey_patch()
#asyncio.set_event_loop(asyncio.new_event_loop())


WORDS = ['the', 'quick', 'brown', 'fox']


app = faust.App(
    'word-counts5',
    url='kafka://localhost:9092',
    default_partitions=6,
    key_serializer='json',
    value_serializer='json',
    store='rocksdb://',
)

posts_topic = app.topic('posts3',
                        value_type=str,
                        value_serializer='raw')
words_topic = app.topic('words3',
                        key_type=str,
                        key_serializer='raw',
                        value_type=str,
                        value_serializer='raw')
other_topic = app.topic('other', value_type=str, value_serializer='raw')

word_counts = app.Table('word_counts3', default=int,
                        help='Keep count of words (str to int).')


@app.agent(posts_topic)
async def shuffle_words(posts):
    async for post in posts:
        print('POST: %r' % (post,))
        for word in post.split():
            await words_topic.send(key=word, value=word)


@app.agent(other_topic)
async def slower_down(words):
    async for i, word in words.enumerate():
        print(f'+SLEEP {i} {word}')
        await asyncio.sleep(1)
        print(f'-SLEEP {i} {word}')

@app.agent(words_topic)
async def count_words(words):
    """Count words from blog post article body."""
    async for word in words:
        print(f'RECEIVED WORD {word}')
        word_counts[word] += 1
        print(f'WORD {word} -> {word_counts[word]}')


@app.page('/count/')
@app.table_route(table=word_counts, shard_param='word')
async def get_count(web, request):
    word = request.GET['word']
    return web.json({
        word: word_counts[word],
    })


@app.task
async def sender():
    for word in WORDS:
        for _ in range(1000):
            await shuffle_words.send(value=word)
            await other_topic.send(value=word)

    await asyncio.sleep(5.0)
    print(word_counts.as_ansitable(
        key='word',
        value='count',
        title='$$ TALLY $$',
        sort=True,
    ))


if __name__ == '__main__':
    app.main()
