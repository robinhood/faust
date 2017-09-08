import asyncio
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


class Word(faust.Record):
    value: str


posts_topic = app.topic('posts2',
                        value_type=str,
                        value_serializer='raw')
words_topic = app.topic('words2',
                        key_type=str,
                        key_serializer='raw',
                        value_type=str,
                        value_serializer='raw')

word_counts = app.Table('word_counts2', default=int)


@app.actor(posts_topic)
async def shuffle_words(posts):
    async for post in posts:
        for word in post.split():
            await words_topic.send(key=word, value=word)


@app.actor(words_topic)
async def count_words(words):
    async for word in words:
        word_counts[word] += 1
        print(f'WORD {word} -> {word_counts[word]}')


@app.actor()
async def sender(stream):
    for i in range(100):
        await shuffle_words.send(value=random.choice(WORDS))
    await asyncio.sleep(10)
    for word in WORDS: print('AWAITING WORD: %r' % (word,))
