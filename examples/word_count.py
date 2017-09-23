import asyncio
import click
import faust
import random

WORDS = ['the', 'quick', 'brown', 'fox']


app = faust.App(
    'word-count2',
    url='kafka://localhost:9092',
    default_partitions=6,
    key_serializer='json',
    value_serializer='json',
    store='rocksdb://',
)

posts_topic = app.topic('posts2',
                        value_type=str,
                        value_serializer='raw')
words_topic = app.topic('words2',
                        key_type=str,
                        key_serializer='raw',
                        value_type=str,
                        value_serializer='raw')

word_counts = app.Table('word_counts2', default=int)

crashes = [0]


@app.actor(posts_topic)
async def shuffle_words(posts):
    async for post in posts:
        if crashes[0] < 2:
            crashes[0] += 1
            raise RuntimeError('foo')
        for word in post.split():
            await words_topic.send(key=word, value=word)


@app.actor(words_topic)
async def count_words(words):
    async for word in words:
        word_counts[word] += 1
        print(f'WORD {word} -> {word_counts[word]}')


@app.page('/count/')
async def get_count(web, request):
    return web.json({
        'counts': dict(word_counts),
    })


@app.actor()
async def sender(stream):
    for i in range(100):
        await shuffle_words.send(value=random.choice(WORDS))
    await asyncio.sleep(10)
    for word in WORDS: print('AWAITING WORD: %r' % (word,))


@app.command(click.argument('rest'), click.option('--foo/--no-foo', default=False))
async def produce(self, rest: str, foo: bool):
    """Produce example data."""
    print(f'Hello: {foo}: {rest}')



if __name__ == '__main__':
    app.main()
