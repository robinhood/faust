.. _guide-tasks:

======================================================
 Tasks, Timers, Cron Jobs, Web Views, and CLI Commands
======================================================

.. contents::
    :local:
    :depth: 1

.. module:: faust

.. currentmodule:: faust

.. _tasks-basics:

Tasks
=====

Your application will have agents that process events in streams, but
can also start :class:`asyncio.Task`-s that do other things,
like periodic timers, views for the embedded web server, or additional
command-line commands.

Decorating an async function with the `@app.task` decorator will
tell the worker to start that function as soon as the worker is fully
operational:

.. sourcecode:: python

    @app.task
    async def on_started():
        print('APP STARTED')

If you add the above to the module that defines your app and start the worker,
you should see the message printed in the output of the worker.

A task is a one-off task; if you want to do something at periodic
intervals, you can use a timer.

.. _tasks-timers:

Timers
======

A timer is a task that executes every ``n`` seconds:

.. sourcecode:: python

    @app.timer(interval=60.0)
    async def every_minute():
        print('WAKE UP')


After starting the worker, and it's operational, the above timer will print
something every minute.

.. _tasks-cron-jobs:

Cron Jobs
=========

A cron job is a task that executes according to a crontab format,
usually at fixed times:

.. sourcecode:: python

    @app.crontab('0 20 * * *')
    async def every_dat_at_8_pm():
        print('WAKE UP ONCE A DAY')


After starting the worker, and it's operational, the above cron job will print
something every day at 8pm.

``crontab`` takes 1 mandatory argument ``cron_format`` and 2 optional arguments:

- ``tz``, represents the timezone. Defaults to None which gives behaves as UTC.
- ``on_leader``, boolean defaults to False, only run on leader?

.. sourcecode:: python

    @app.crontab('0 20 * * *', tz=pytz.timezone('US/Pacific'), on_leader=True)
    async def every_dat_at_8_pm_pacific():
        print('WAKE UP AT 8:00pm PACIFIC TIME ONLY ON THE LEADER WORKER')


.. _tasks-web-views:

Web Views
=========

The Faust worker will also expose a web server on every instance,
that by default runs on port 6066. You can access this in your web browser
after starting a worker instance on your local machine:

.. sourcecode:: console

    $ faust -A myapp worker -l info

Just point your browser to the local port to see statistics about your
running instance:

.. sourcecode:: text

    http://localhost:6066

You can define additional views for the web server (called pages). The server
will use the :pypi:`aiohttp` HTTP server library, but you can also
write custom web server drivers.

Add a simple page returning a JSON structure by adding this to your app module:

.. sourcecode:: python

    # this counter exists in-memory only,
    # so will be wiped when the worker restarts.
    count = [0]

    @app.page('/count/')
    async def get_count(self, request):
        # update the counter
        count[0] += 1
        # and return it.
        return self.json({
            'count': count[0],
        })


This example view is of limited usefulness. It only provides you with
a count of how many times the page is requested, on that particular server,
for as long as it's up, but you can also call actors or access table
data in web views.

Restart your Faust worker, and you can visit your new page at:

.. sourcecode:: text

    http://localhost:6066/count/

Your workers may have an arbitrary number of views, and it's up to you what
they provide. Just like other web apps they can communicate with Redis,
SQL databases, and so on. Anything you want, really, and it's executing
in an asynchronous event loop.

You can decide to develop your web app directly in the Faust workers, or you
may choose to keep your regular web server separate from your Faust workers.

You can create complex systems quickly, just by putting everything in a single
Faust app.

HTTP Verbs: ``GET``/``POST``/``PUT``/``DELETE``
===============================================

Specify a :class:`faust.web.View` class when you need to handle HTTP
verbs other than ``GET``:

.. sourcecode:: python

    from faust.web import Request, Response, View

    @app.page('/count/')
    class counter(View):

        count: int = 0

        async def get(self, request: Request) -> Response
            return self.json({'count': self.count})

        async def post(self, request: Request) -> Response:
            n: int = request.query['n']
            self.count += 1
            return self.json({'count': self.count})

        async def delete(self, request: Request) -> Response:
            self.count = 0

Exposing Tables
---------------

A frequent requirement is the ability to expose table values in a web view,
and while this is likely to be built-in to Faust in the future,
you will have to implement this manually for now.

Tables are partitioned by key, and data for any specific key will exist
on a particular worker instance. You can use the ``@app.table_route``
decorator to reroute the request to the worker holding that partition.

We define our table, and an agent reading from the stream to populate the table:

.. sourcecode:: python

    import faust

    app = faust.App(
        'word-counts',
        broker='kafka://localhost:9092',
        store='rocksdb://',
        topic_partitions=8,
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

After that we define the view, using the ``@app.table_route`` decorator to
reroute the request to the correct worker instance:

.. sourcecode:: python

    @app.page('/count/{word}/')
    @app.table_route(table=word_counts, match_info='word')
    async def get_count(web, request, word):
        return web.json({
            word: word_counts[word],
        })

In the above example we used part of the URL to find the given word,
but you may also want to get this from query parameters.

Table route based on key in query parameter:

.. sourcecode:: python

    @app.page('/count/')
    @app.table_route(table=word_counts, query_param='word')
    async def get_count(web, request):
        word = request.query['word']
        return web.json({
            word: word_counts[word],
        })

.. _tasks-cli-commands:

CLI Commands
============

As you may already know, you can make your project into an executable,
that can start Faust workers, list agents, models and more,
just by calling ``app.main()``.

Even if you don't do that, the :program:`faust` program is always available
and you can point it to any app:

.. sourcecode:: console

    $ faust -A myapp worker -l info


The ``myapp`` argument should point to a Python module/package having
an ``app`` attribute.  If the attribute has a different name, please specify
a fully qualified path:

.. sourcecode:: console

    $ faust -A myproj.apps:faust_app worker -l info

Do ``--help`` to get a list of subcommands supported by the app:

.. sourcecode:: console

    $ faust -A myapp --help

To turn your script into the :program:`faust` command, with the
:option:`-A <faust -A>` option already set, add this to the end of the module:

.. sourcecode:: python

    if __name__ == '__main__':
        app.main()

If saved as :file:`simple.py` you can now execute it as if it was
the :program:`faust` program:

.. sourcecode:: console

    $ python simple.py worker -l info

Custom CLI Commands
-------------------

To add a custom command to your app, see the :file:`examples/simple.py`
example in the Faust distribution, where we've added a ``produce`` command
used to send example data into the stream processors:

.. sourcecode:: python

    from faust.cli import option

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

The ``@app.command`` decorator accepts both :class:`click.option` and
:class:`click.argument`, so you can specify command-line options, as
well as command-line positional arguments.

Daemon Commands
~~~~~~~~~~~~~~~

The ``daemon`` flag can be set to mark the command as a background service
that won't exit until the user hits :kbd:`Control-c`, or the process is
terminated by another signal:

.. sourcecode:: python

    @app.command(
        option('--foo', type=float, default=1.33),
        daemon=True,
    )
    async def my_daemon(self, foo: float):
        print('STARTING DAEMON')
        ...
        # set up some stuff
        # we can return here but the program will not shut down
        # until the user hits :kbd:`Control-c`, or the process is terminated
        # by signal
        return
