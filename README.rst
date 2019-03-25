.. XXX Need to change this image to readthedocs before release

.. image:: https://raw.githubusercontent.com/robinhood/faust/8ee5e209322d9edf5bdb79b992ef986be2de4bb4/artwork/banner-alt1.png

===========================
 Python Stream Processing
===========================

|build-status| |coverage| |license| |wheel| |pyversion| |pyimp|

:Version: 1.5.1
:Web: http://faust.readthedocs.io/
:Download: http://pypi.org/project/faust
:Source: http://github.com/robinhood/faust
:Keywords: distributed, stream, async, processing, data, queue


.. sourcecode:: python

    # Python Streams
    # Forever scalable event processing & in-memory durable K/V store;
    # as a library w/ asyncio & static typing.
    import faust

**Faust** is a stream processing library, porting the ideas from
`Kafka Streams`_ to Python.

It is used at `Robinhood`_ to build high performance distributed systems
and real-time data pipelines that process billions of events every day.

Faust provides both *stream processing* and *event processing*,
sharing similarity with tools such as
`Kafka Streams`_, `Apache Spark`_/`Storm`_/`Samza`_/`Flink`_,

It does not use a DSL, it's just Python!
This means you can use all your favorite Python libraries
when stream processing: NumPy, PyTorch, Pandas, NLTK, Django,
Flask, SQLAlchemy, ++

Faust requires Python 3.6 or later for the new `async/await`_ syntax,
and variable type annotations.

Here's an example processing a stream of incoming orders:

.. sourcecode:: python

    app = faust.App('myapp', broker='kafka://localhost')

    # Models describe how messages are serialized:
    # {"account_id": "3fae-...", amount": 3}
    class Order(faust.Record):
        account_id: str
        amount: int

    @app.agent(value_type=Order)
    async def order(orders):
        async for order in orders:
            # process infinite stream of orders.
            print(f'Order for {order.account_id}: {order.amount}')

The Agent decorator defines a "stream processor" that essentially
consumes from a Kafka topic and does something for every event it receives.

The agent is an ``async def`` function, so can also perform
other operations asynchronously, such as web requests.

This system can persist state, acting like a database.
Tables are named distributed key/value stores you can use
as regular Python dictionaries.

Tables are stored locally on each machine using a superfast
embedded database written in C++, called `RocksDB`_.

Tables can also store aggregate counts that are optionally "windowed"
so you can keep track
of "number of clicks from the last day," or
"number of clicks in the last hour." for example. Like `Kafka Streams`_,
we support tumbling, hopping and sliding windows of time, and old windows
can be expired to stop data from filling up.

For reliability we use a Kafka topic as "write-ahead-log".
Whenever a key is changed we publish to the changelog.
Standby nodes consume from this changelog to keep an exact replica
of the data and enables instant recovery should any of the nodes fail.

To the user a table is just a dictionary, but data is persisted between
restarts and replicated across nodes so on failover other nodes can take over
automatically.

You can count page views by URL:

.. sourcecode:: python

    # data sent to 'clicks' topic sharded by URL key.
    # e.g. key="http://example.com" value="1"
    click_topic = app.topic('clicks', key_type=str, value_type=int)

    # default value for missing URL will be 0 with `default=int`
    counts = app.Table('click_counts', default=int)

    @app.agent(click_topic)
    async def count_click(clicks):
        async for url, count in clicks.items():
            counts[url] += count

The data sent to the Kafka topic is partitioned, which means
the clicks will be sharded by URL in such a way that every count
for the same URL will be delivered to the same Faust worker instance.


Faust supports any type of stream data: bytes, Unicode and serialized
structures, but also comes with "Models" that use modern Python
syntax to describe how keys and values in streams are serialized:

.. sourcecode:: python

    # Order is a json serialized dictionary,
    # having these fields:

    class Order(faust.Record):
        account_id: str
        product_id: str
        price: float
        quantity: float = 1.0

    orders_topic = app.topic('orders', key_type=str, value_type=Order)

    @app.agent(orders_topic)
    async def process_order(orders):
        async for order in orders:
            # process each order using regular Python
            total_price = order.price * order.quantity
            await send_order_received_email(order.account_id, order)

Faust is statically typed, using the ``mypy`` type checker,
so you can take advantage of static types when writing applications.

The Faust source code is small, well organized, and serves as a good
resource for learning the implementation of `Kafka Streams`_.

**Learn more about Faust in the** `introduction`_ **introduction page**
    to read more about Faust, system requirements, installation instructions,
    community resources, and more.

**or go directly to the** `quickstart`_ **tutorial**
    to see Faust in action by programming a streaming application.

**then explore the** `User Guide`_
    for in-depth information organized by topic.

.. _`Robinhood`: http://robinhood.com
.. _`async/await`:
    https://medium.freecodecamp.org/a-guide-to-asynchronous-programming-in-python-with-asyncio-232e2afa44f6
.. _`Celery`: http://celeryproject.org
.. _`Kafka Streams`: https://kafka.apache.org/documentation/streams
.. _`Apache Spark`: http://spark.apache.org
.. _`Storm`: http://storm.apache.org
.. _`Samza`: http://samza.apache.org
.. _`Flink`: http://flink.apache.org
.. _`RocksDB`: http://rocksdb.org
.. _`Apache Kafka`: https://kafka.apache.org

.. _`introduction`: http://faust.readthedocs.io/en/latest/introduction.html

.. _`quickstart`: http://faust.readthedocs.io/en/latest/playbooks/quickstart.html

.. _`User Guide`: http://faust.readthedocs.io/en/latest/userguide/index.html

Faust is...
===========

**Simple**
    Faust is extremely easy to use. To get started using other stream processing
    solutions you have complicated hello-world projects, and
    infrastructure requirements.  Faust only requires Kafka,
    the rest is just Python, so If you know Python you can already use Faust to do
    stream processing, and it can integrate with just about anything.

    Here's one of the easier applications you can make::

        import faust

        class Greeting(faust.Record):
            from_name: str
            to_name: str

        app = faust.App('hello-app', broker='kafka://localhost')
        topic = app.topic('hello-topic', value_type=Greeting)

        @app.agent(topic)
        async def hello(greetings):
            async for greeting in greetings:
                print(f'Hello from {greeting.from_name} to {greeting.to_name}')

        @app.timer(interval=1.0)
        async def example_sender(app):
            await hello.send(
                value=Greeting(from_name='Faust', to_name='you'),
            )

        if __name__ == '__main__':
            app.main()

    You're probably a bit intimidated by the `async` and `await` keywords,
    but you don't have to know how ``asyncio`` works to use
    Faust: just mimic the examples, and you'll be fine.

    The example application starts two tasks: one is processing a stream,
    the other is a background thread sending events to that stream.
    In a real-life application, your system will publish
    events to Kafka topics that your processors can consume from,
    and the background thread is only needed to feed data into our
    example.

**Highly Available**
    Faust is highly available and can survive network problems and server
    crashes.  In the case of node failure, it can automatically recover,
    and tables have standby nodes that will take over.

**Distributed**
    Start more instances of your application as needed.

**Fast**
    A single-core Faust worker instance can already process tens of thousands
    of events every second, and we are reasonably confident that throughput will
    increase once we can support a more optimized Kafka client.

**Flexible**
    Faust is just Python, and a stream is an infinite asynchronous iterator.
    If you know how to use Python, you already know how to use Faust,
    and it works with your favorite Python libraries like Django, Flask,
    SQLAlchemy, NTLK, NumPy, Scikit, TensorFlow, etc.

.. _`introduction`: http://faust.readthedocs.io/en/latest/introduction.html

.. _`quickstart`: http://faust.readthedocs.io/en/latest/playbooks/quickstart.html

.. _`User Guide`: http://faust.readthedocs.io/en/latest/userguide/index.html

Installation
============

You can install Faust either via the Python Package Index (PyPI)
or from source.

To install using `pip`:

.. sourcecode:: console

    $ pip install -U faust

.. _bundles:

Bundles
-------

Faust also defines a group of ``setuptools`` extensions that can be used
to install Faust and the dependencies for a given feature.

You can specify these in your requirements or on the ``pip``
command-line by using brackets. Separate multiple bundles using the comma:

.. sourcecode:: console

    $ pip install "faust[rocksdb]"

    $ pip install "faust[rocksdb,uvloop,fast,redis]"

The following bundles are available:

Stores
~~~~~~

:``faust[rocksdb]``:
    for using `RocksDB`_ for storing Faust table state.

    **Recommended in production.**


.. _`RocksDB`: http://rocksdb.org

Caching
~~~~~~~

:``faust[redis]``:
    for using `Redis_` as a simple caching backend (memcache-style).

Optimization
~~~~~~~~~~~~

:``faust[fast]``:
    for installing all the available C speedup extensions to Faust core.

Sensors
~~~~~~~

:``faust[datadog]``:
    for using the Datadog Faust monitor.

:``faust[statsd]``:
    for using the Statsd Faust monitor.

Event Loops
~~~~~~~~~~~

:``faust[uvloop]``:
    for using Faust with ``uvloop``.

:``faust[gevent]``:
    for using Faust with ``gevent``.

:``faust[eventlet]``:
    for using Faust with ``eventlet``

Debugging
~~~~~~~~~

:``faust[debug]``:
    for using ``aiomonitor`` to connect and debug a running Faust worker.

:``faust[setproctitle]``:
    when the ``setproctitle`` module is installed the Faust worker will
    use it to set a nicer process name in ``ps``/``top`` listings.
    Also installed with the ``fast`` and ``debug`` bundles.

Downloading and installing from source
--------------------------------------

Download the latest version of Faust from
http://pypi.org/project/faust

You can install it by doing:

.. sourcecode:: console

    $ tar xvfz faust-0.0.0.tar.gz
    $ cd faust-0.0.0
    $ python setup.py build
    # python setup.py install

The last command must be executed as a privileged user if
you are not currently using a virtualenv.

Using the development version
-----------------------------

With pip
~~~~~~~~

You can install the latest snapshot of Faust using the following
``pip`` command:

.. sourcecode:: console

    $ pip install https://github.com/robinhood/faust/zipball/master#egg=faust

.. _`introduction`: http://faust.readthedocs.io/en/latest/introduction.html

.. _`quickstart`: http://faust.readthedocs.io/en/latest/playbooks/quickstart.html

.. _`User Guide`: http://faust.readthedocs.io/en/latest/userguide/index.html

FAQ
===

Can I use Faust with Django/Flask/etc.?
---------------------------------------

Yes! Use ``gevent`` or ``eventlet`` as a bridge to integrate with
``asyncio``.

Using ``gevent``
~~~~~~~~~~~~~~~~~~~~

This approach works with any blocking Python library that can work
with ``gevent``.

Using ``gevent`` requires you to install the ``aiogevent`` module,
and you can install this as a bundle with Faust:

.. sourcecode:: console

    $ pip install -U faust[gevent]

Then to actually use ``gevent`` as the event loop you have to either
use the ``-L <faust --loop>`` option to the ``faust`` program:

.. sourcecode:: console

    $ faust -L gevent -A myproj worker -l info

or add ``import mode.loop.gevent`` at the top of your entry point script:

.. sourcecode:: python

    #!/usr/bin/env python3
    import mode.loop.gevent

REMEMBER: It's very important that this is at the very top of the module,
and that it executes before you import libraries.


Using ``eventlet``
~~~~~~~~~~~~~~~~~~~~~~

This approach works with any blocking Python library that can work with
``eventlet``.

Using ``eventlet`` requires you to install the ``aioeventlet`` module,
and you can install this as a bundle along with Faust:

.. sourcecode:: console

    $ pip install -U faust[eventlet]

Then to actually use eventlet as the event loop you have to either
use the ``-L <faust --loop>`` argument to the ``faust`` program:

.. sourcecode:: console

    $ faust -L eventlet -A myproj worker -l info

or add ``import mode.loop.eventlet`` at the top of your entry point script:

.. sourcecode:: python

    #!/usr/bin/env python3
    import mode.loop.eventlet  # noqa

.. warning::

    It's very important this is at the very top of the module,
    and that it executes before you import libraries.

Can I use Faust with Tornado?
-----------------------------

Yes! Use the ``tornado.platform.asyncio`` bridge:
http://www.tornadoweb.org/en/stable/asyncio.html

Can I use Faust with Twisted?
-----------------------------

Yes! Use the ``asyncio`` reactor implementation:
https://twistedmatrix.com/documents/17.1.0/api/twisted.internet.asyncioreactor.html

Will you support Python 3.5 or earlier?
---------------------------------------

There are no immediate plans to support Python 3.5, but you are welcome to
contribute to the project.

Here are some of the steps required to accomplish this:

- Source code transformation to rewrite variable annotations to comments

  for example, the code::

        class Point:
            x: int = 0
            y: int = 0

   must be rewritten into::

        class Point:
            x = 0  # type: int
            y = 0  # type: int

- Source code transformation to rewrite async functions

    for example, the code::

        async def foo():
            await asyncio.sleep(1.0)

    must be rewritten into::

        @coroutine
        def foo():
            yield from asyncio.sleep(1.0)

Will you support Python 2?
--------------------------

There are no plans to support Python 2, but you are welcome to contribute to
the project (details in the question above is relevant also for Python 2).


I get a maximum number of open files exceeded error by RocksDB when running a Faust app locally. How can I fix this?
--------------------------------------------------------------------------------------------------------------------

You may need to increase the limit for the maximum number of open files. The
following post explains how to do so on OS X:
https://blog.dekstroza.io/ulimit-shenanigans-on-osx-el-capitan/


What kafka versions faust supports?
---------------------------------------

Faust supports kafka with version >= 0.10.

.. _`introduction`: http://faust.readthedocs.io/en/latest/introduction.html

.. _`quickstart`: http://faust.readthedocs.io/en/latest/playbooks/quickstart.html

.. _`User Guide`: http://faust.readthedocs.io/en/latest/userguide/index.html

.. _getting-help:

Getting Help
============

.. _mailing-list:

Mailing list
------------

For discussions about the usage, development, and future of Faust,
please join the `faust-users`_ mailing list.

.. _`faust-users`: https://groups.google.com/group/faust-users/

.. _slack-channel:

Slack
-----

Come chat with us on Slack:

https://join.slack.com/t/fauststream/shared_invite/enQtNDEzMTIyMTUyNzU2LTRkM2Q2ODkwZTk5MzczNmUxOGU0NWYxNzA2YzYwNTAyZmRiOTRmMzkyMDk0ODY2MjIzOTg2NGI0ODlmNTYxNTc

Resources
=========

.. _bug-tracker:

Bug tracker
-----------

If you have any suggestions, bug reports, or annoyances please report them
to our issue tracker at https://github.com/robinhood/faust/issues/

.. _wiki:

Wiki
----

https://wiki.github.com/robinhood/faust/

.. _license:

License
=======

This software is licensed under the `New BSD License`. See the ``LICENSE``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround

.. _`introduction`: http://faust.readthedocs.io/en/latest/introduction.html

.. _`quickstart`: http://faust.readthedocs.io/en/latest/playbooks/quickstart.html

.. _`User Guide`: http://faust.readthedocs.io/en/latest/userguide/index.html

Contributing
============

Development of `Faust` happens at GitHub: https://github.com/robinhood/faust

You're highly encouraged to participate in the development
of `Faust`.

Be sure to also read the `Contributing to Faust`_ section in the
documentation.

.. _`Contributing to Faust`:
    http://faust.readthedocs.io/en/latest/contributing.html

Code of Conduct
===============

Everyone interacting in the project's codebases, issue trackers, chat rooms,
and mailing lists is expected to follow the Faust Code of Conduct.

As contributors and maintainers of these projects, and in the interest of fostering
an open and welcoming community, we pledge to respect all people who contribute
through reporting issues, posting feature requests, updating documentation,
submitting pull requests or patches, and other activities.

We are committed to making participation in these projects a harassment-free
experience for everyone, regardless of level of experience, gender,
gender identity and expression, sexual orientation, disability,
personal appearance, body size, race, ethnicity, age,
religion, or nationality.

Examples of unacceptable behavior by participants include:

* The use of sexualized language or imagery
* Personal attacks
* Trolling or insulting/derogatory comments
* Public or private harassment
* Publishing other's private information, such as physical
  or electronic addresses, without explicit permission
* Other unethical or unprofessional conduct.

Project maintainers have the right and responsibility to remove, edit, or reject
comments, commits, code, wiki edits, issues, and other contributions that are
not aligned to this Code of Conduct. By adopting this Code of Conduct,
project maintainers commit themselves to fairly and consistently applying
these principles to every aspect of managing this project. Project maintainers
who do not follow or enforce the Code of Conduct may be permanently removed from
the project team.

This code of conduct applies both within project spaces and in public spaces
when an individual is representing the project or its community.

Instances of abusive, harassing, or otherwise unacceptable behavior may be
reported by opening an issue or contacting one or more of the project maintainers.

This Code of Conduct is adapted from the Contributor Covenant,
version 1.2.0 available at http://contributor-covenant.org/version/1/2/0/.

.. _`introduction`: http://faust.readthedocs.io/en/latest/introduction.html

.. _`quickstart`: http://faust.readthedocs.io/en/latest/playbooks/quickstart.html

.. _`User Guide`: http://faust.readthedocs.io/en/latest/userguide/index.html

.. |build-status| image:: https://secure.travis-ci.org/robinhood/faust.png?branch=master
    :alt: Build status
    :target: https://travis-ci.org/robinhood/faust

.. |coverage| image:: https://codecov.io/github/robinhood/faust/coverage.svg?branch=master
    :target: https://codecov.io/github/robinhood/faust?branch=master

.. |license| image:: https://img.shields.io/pypi/l/faust.svg
    :alt: BSD License
    :target: https://opensource.org/licenses/BSD-3-Clause

.. |wheel| image:: https://img.shields.io/pypi/wheel/faust.svg
    :alt: faust can be installed via wheel
    :target: http://pypi.org/project/faust/

.. |pyversion| image:: https://img.shields.io/pypi/pyversions/faust.svg
    :alt: Supported Python versions.
    :target: http://pypi.org/project/faust/

.. |pyimp| image:: https://img.shields.io/pypi/implementation/faust.svg
    :alt: Support Python implementations.
    :target: http://pypi.org/project/faust/

.. _`introduction`: http://faust.readthedocs.io/en/latest/introduction.html

.. _`quickstart`: http://faust.readthedocs.io/en/latest/playbooks/quickstart.html

.. _`User Guide`: http://faust.readthedocs.io/en/latest/userguide/index.html

