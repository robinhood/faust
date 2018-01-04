.. _guide-tasks:

============================================
 Tasks, Timers, Web Views, and CLI Commands
============================================

.. contents::
    :local:
    :depth: 1

.. module:: faust

.. currentmodule:: faust

.. _task-basics:

Tasks
=====

Your application will have agents that process events in streams, but
your application can also start asyncio.Tasks that do other things, like periodic timers,
views for the embedded web server, or additional command-line commands.

Decorating an async function with the `@app.task` decorator will
tell the worker to start that function as soon as the worker is fully
operational:

.. sourcecode:: python

    @app.task
    async def on_started():
        print('APP STARTED')

If you add the above to the module that defines your app and start the worker,
you should see the message printed after starting the worker.

A task is a one-off task, if you want to do something at periodic intervals
you can use a timer.

.. _tasks-timers:

Timers
======

A timer is also a task, but one that executes every ``n`` seconds:

.. sourcecode:: python

    @app.timer(interval=60.0)
    async def every_minute():
        print('WAKE UP')


The above timer will print something every minute, starting from one minute
after the worker is started and fully operational.

.. _tasks-web-views:

Web Views
=========

The Faust worker also exposes a webserver on every instance,
and this server will by default be running on port 6066,
so after starting a worker instance on your local machine:

.. sourcecode:: console

    $ faust -A myapp worker -l info

You can visit the web server in your browser,
to be presented with statistics about your running instance:

.. sourcecode:: text

    http://localhost:6066

Your app may also add additional views to be exposed by the webserver.
The server is using :pypi:`aiohttp` by default, but you may implement
additional web server drivers should you want to use something different.

To expose a simple view returning a JSON structure you can add the
following code to your app module:

.. sourcecode:: python

    # this counter exists in-memory only,
    # so will be wiped when the worker restarts.
    count = [0]

    @app.page('/count/')
    async def get_count(web, request):
        # update the counter
        count[0] + 1
        # and return it.
        return web.json({
            'count': count[0],
        })


This example view is pretty useless, as it's only providing you with
a count of how many times the view was requested (on that particular server,
for as long as it's up).

Restart your Faust worker, and you can visit your new page at:

.. sourcecode:: text

    http://localhost:6066/count/

Your workers may have arbitrary views, and it's up to you what they provide.
They could be communicating with Redis, or an SQL database just like other
web apps.  You may decide to develop your web app directly in the Faust
workers, or you may decide to keep your normal web server separate from your Faust
workers.  The choice is up to you, but you can definitely create very
complicated systems very easily, by keeping everything in a Faust app.

Exposing Tables
---------------

One common need, is to expose table values in a web view.
Since tables are routed by key, and the data for one key may exist
on a specific worker instance, you need to reroute the request to the
instance having that partition, using the ``@app.table_route`` decorator.

First we define our table, and the agent that reads the stream and populates
the table:

.. sourcecode:: python

    import faust

    app = faust.App(
        'word-counts',
        url='kafka://localhost:9092',
        default_partitions=8,
        store='rocksdb://',
    )

    posts_topic = app.topic('posts', value_type=str)
    word_counts = app.Table('word_counts', default=int,
                             help='Keep count of words (str to int).')


    class Word(faust.Record):
        word: str

    @app.agent(posts_topic)
    async def shuffle_words(posts):
        async for post in posts:
            for word in post.split():
                await count_words.send(key=word, value=Word(word=word))

    @app.agent()
    async def count_words(words):
        """Count words from blog post article body."""
        async for word in words:
            word_counts[word.word] += 1

Then we define our view, using the ``@app.table_route`` decorator to route
the request to the correct worker instance:

.. sourcecode:: python

    @app.page('/count/')
    @app.table_route(table=word_counts, shard_param='word')
    async def get_count(web, request):
        word = request.GET['word']
        return web.json({
            word.word: word_counts[word.word],
        })

.. _tasks-cli-commands:

CLI Commands
============

As you may already know, you can make your app into an executable, that
can start Faust workers, list agents, models and more.

The :program:`faust` command is always avaialable, and you can point it to any
app:

.. sourcecode:: console

    $ faust -A myapp worker -l info

To get a list of subcommands supported by the app, you can execute:

.. sourcecode:: console

    $ faust -A myapp --help

To turn your script into the faust command, with the app already set.
For example for :file:`examples/simple.py` in the Faust distribution,
you can add the following to the end:

.. sourcecode:: python

    if __name__ == '__main__':
        app.main()

and you can now execute :file:`examples.simple` as if it was the
:program:`faust` program:

.. sourcecode:: console

    $ python examples/main.py worker -l info

Custom CLI Commands
-------------------

To add a custom command to your app, see for example the
:file:`examples/simple.py` file, where an additional `produce` command is
added to send example data into the stream processors:

.. sourcecode:: python

    from click import option

    # the full example is in examples/simple.py in the Faust distribution.
    # this only shows the command part of this code.

    @app.command(
        option('--max-latency',
               type=float, default=PRODUCE_LATENCY,
               help='Add delay of (at most) n seconds between publishing.'),
        option('--max-messages',
               type=int, default=None,
               help='Send at most N messages or 0 for infinity.'),
    )
    async def produce(self, max_latency: float, max_messages: int):
        """Produce example Withdrawal events."""
        num_countries = 5
        countries = [f'country_{i}' for i in range(num_countries)]
        country_dist = [0.9] + ([0.10 / num_countries] * (num_countries - 1))
        num_users = 500
        users = [f'user_{i}' for i in range(num_users)]
        self.say('Done setting up. SENDING!')
        for i in range(max_messages) if max_messages is not None else count():
            withdrawal = Withdrawal(
                user=random.choice(users),
                amount=random.uniform(0, 25_000),
                country=random.choices(countries, country_dist)[0],
                date=datetime.utcnow().replace(tzinfo=timezone.utc),
            )
            await withdrawals_topic.send(key=withdrawal.user, value=withdrawal)
            if not i % 10000:
                self.say(f'+SEND {i}')
            if max_latency:
                await asyncio.sleep(random.uniform(0, max_latency))

The ``@app.command`` decorator takes both :class:`click.option` and
:class:`click.argument`, so you can both add custom command-line options, as
well as command-line positional arguments.
