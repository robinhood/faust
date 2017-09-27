import random
import faust

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

word_counts = app.Table('word_counts2', default=int,
                        help='Keep count of words (str to int).')


@app.agent(posts_topic)
async def shuffle_words(posts):
    async for post in posts:
        for word in post.split():
            await words_topic.send(key=word, value=word)


@app.agent(words_topic)
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
    for _ in range(100):
        await shuffle_words.send(value=random.choice(WORDS))


if __name__ == '__main__':
    app.main()
