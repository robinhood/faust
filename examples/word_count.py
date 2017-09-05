import faust


app = faust.App(
    'word-count',
    url='kafka://localhost:9092',
    default_partitions=6,
)

posts_topic = app.topic('posts', value_type=str)
words_topic = app.topic('words', value_type=str)

word_counts = app.Table('word_counts', default=int)


@app.actor(posts_topic)
async def shuffle_words(posts):
    async for post in posts:
        for word in post.split():
            await words_topic.send(key=word, value=word)


@app.actor(words_topic)
async def count_words(words):
    async for word in words:
        word_counts[word] += 1
