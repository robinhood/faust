import asyncio
from operator import itemgetter
import faust
from terminaltables import SingleTable

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

word_counts = app.Table('word_counts3', default=int,
                        help='Keep count of words (str to int).')


@app.actor(posts_topic)
async def shuffle_words(posts):
    async for post in posts:
        for word in post.split():
            await words_topic.send(key=word, value=word)


@app.actor(words_topic)
async def count_words(words):
    """Count words from blog post article body."""
    async for word in words:
        word_counts[word] += 1
        print(f'WORD {word} -> {word_counts[word]}')


@app.page('/count/')
async def get_count(web, request):
    return web.json({
        'counts': dict(word_counts),
    })


@app.task
async def sender():
    for word in WORDS:
        for _ in range(30):
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
