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
