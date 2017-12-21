=====================================================================
 Faust: Stream Processing for Python
=====================================================================

|build-status| |license| |wheel| |pyversion| |pyimp|

:Version: 0.9.20
:Web: http://fauststream.com
:Download: http://pypi.python.org/pypi/faust
:Source: http://github.com/fauststream/faust
:Keywords: distributed, stream, async, processing, data, queue

Faust is a Python library for event processing and streaming applications
that are distributed and fault-tolerant.

It's inspired by tools such as `Kafka Streams`_, `Apache Spark`_,
`Apache Storm`_, `Apache Samza`_ and `Apache Flink`_; but takes
a radically much simpler approach to stream processing.

Modern web applications are increasingly being written as a collection
of microservices and even before this it has been difficult to write
data reporting operations at scale.  In a reactive stream based system,
you don't have to strain your database with costly queries, instead a streaming
data pipeline updates information as events happen in your system, in real-time.

Faust also enables you to take advantage of asyncio and asynchronous
processing, moving complicated and costly operations outside
of the web server process: converting video, notifying third-party services,
etc. are common use cases for event processing.

You may not know it yet, but if you're writing a modern web application,
you probably already have a need for Faust.

.. _`Kafka Streams`: https://kafka.apache.org/documentation/streams
.. _`Apache Spark`: http://spark.apache.org
.. _`Apache Storm`: http://storm.apache.org
.. _`Apache Flink`: http://flink.apache.org
.. _`Apache Samza`: http://samza.apache.org

Faust is...
==========================

**Simple**
    Faust is extremely easy to use compared to other stream processing
    solutions.  There's no DSL to limit your creativity, no restricted
    set of operations to work from, and since Faust is a library it can
    integrate with just about anything.

    Here's one of the simplest applications you can make::

        import faust

        class Greeting(faust.Record):
            from_name: str
            to_name: str

        app = faust.App('hello-app', url='kafka://localhost')
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
    but you don't have to know how asyncio works to use
    Faust: just mimic the examples and you'll be fine.

    The example application starts two tasks: one is processing a stream,
    the other is a background thread sending events to that stream.
    In a real-live application your system will publish
    events to Kafka topics that your processors can consume from,
    and the background thread is only needed to feed data into our
    example.

**Highly Available**
    Faust is highly available and can survive network problems and server
    crashes.  In the case of node failure it can automatically recover,
    and tables have standby nodes that will take over.

**Distributed**
    Start more instances of your application as needed.

**Fast**
    Faust applications can hopefully handle millions of events per second
    in the future.

**Flexible**
    Faust is just Python, and a stream is just an infinite async iterator.
    If you know how to use Python, you already know how to use Faust,
    and it works with your favorite Python libraries like Django, Flask,
    SQLAlchemy, NTLK, NumPy, Scikit, TensorFlow, etc.

.. _installation:

Installation
============

You can install faust either via the Python Package Index (PyPI)
or from source.

To install using `pip`,::

    $ pip install -U faust

.. _bundles:

Bundles
-------

Faust also defines a group of setuptools extensions that can be used
to install Faust and the dependencies for a given feature.

You can specify these in your requirements or on the ``pip``
command-line by using brackets. Multiple bundles can be separated by comma:

::


    $ pip install "faust[rocksdb]"

    $ pip install "faust[rocksdb,uvloop,fast]"

The following bundles are available:

Stores
~~~~~~

:``faust[rocksdb]``:
    for using RocksDB for storing Faust table state.

    **Recommended in production.**

Optimization
~~~~~~~~~~~~

:``faust[fast]``:
    for installing all the available C speedup extensions to Faust core.

Sensors
~~~~~~~

:``faust[statsd]``:
    for using the Statsd Faust monitor.

Event Loops
~~~~~~~~~~~

:``faust[uvloop]``:
    for using Faust with ``uvloop``.

Debugging
~~~~~~~~~

:``faust[debug]``:
    for using ``aiomonitor`` to connect and debug a running faust worker.

:``faust[setproctitle]``:
    when the ``setproctitle`` module is installed the Faust worker will
    use it to set a nicer process name in ps/top listings.  Also installed
    with the ``fast`` and ``debug`` bundles.

.. _installing-from-source:

Downloading and installing from source
--------------------------------------

Download the latest version of faust from
http://pypi.python.org/pypi/faust

You can install it by doing the following,::

    $ tar xvfz faust-0.0.0.tar.gz
    $ cd faust-0.0.0
    $ python setup.py build
    # python setup.py install

The last command must be executed as a privileged user if
you are not currently using a virtualenv.

.. _installing-from-git:

Using the development version
-----------------------------

With pip
~~~~~~~~

You can install the latest snapshot of faust using the following
pip command::

    $ pip install https://github.com/fauststream/faust/zipball/master#egg=faust

FAQ
===

Can I use Faust with Django/Flask/etc.?
---------------------------------------

Yes! Use gevent/eventlet and use a bridge to integrate with asyncio.

- ``aiogevent`` enables you to run Faust on top of gevent:

    https://pypi.python.org/pypi/aiogevent

    Example::

        import aiogevent
        import asyncio
        asyncio.set_event_loop_policy(aiogevent.EventLoopPolicy())
        import gevent.monkey
        gevent.monkey.patch_all()
        # if you use PostgreSQL with psycopg, make sure you also
        # install psycogreen and call this pather:
        #  import psycogreen.gevent
        #  psycogreen.gevent.patch_psycopg()

        # Import Django/Flask etc, stuff and use them with Faust.

- ``aioeventlet`` enables you to run Faust on top of eventlet:

    http://aioeventlet.readthedocs.io

    Example::

        import aioeventlet
        import asyncio
        asyncio.set_event_loop_policy(aioeventlet.EventloopPolicy())
        import eventlet
        eventlet.monkey_patch()
        # if you use PostgreSQL with psycopg, make sure you also
        # install psycogreen and call this pather:
        #  import psycogreen.eventlet
        #  psycogreen.eventlet.patch_psycopg()

        # Import Django/Flask etc, stuff and use them with Faust.

Can I use Faust with Tornado?
-----------------------------

Yes! Use the ``tornado.platform.asyncio`` bridge:
http://www.tornadoweb.org/en/stable/asyncio.html

Can I use Faust with Twisted?
-----------------------------

Yes! Use the asyncio reactor implementation:
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
the project (details in question above is relevant also for Python 2).

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

https://fauststream.slack.com

.. _bug-tracker:

Bug tracker
===========

If you have any suggestions, bug reports, or annoyances please report them
to our issue tracker at https://github.com/fauststream/faust/issues/

.. _wiki:

Wiki
====

https://wiki.github.com/fauststream/faust/

.. _contributing-short:

Contributing
============

Development of `faust` happens at GitHub: https://github.com/fauststream/faust

You're highly encouraged to participate in the development
of `faust`.

Be sure to also read the `Contributing to Faust`_ section in the
documentation.

.. _`Contributing to Faust`:
    http://docs.fauststream.com/en/master/contributing.html

.. _license:

License
=======

This software is licensed under the `New BSD License`. See the ``LICENSE``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround

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

.. |build-status| image:: https://secure.travis-ci.org/fauststream/faust.png?branch=master
    :alt: Build status
    :target: https://travis-ci.org/fauststream/faust

.. |license| image:: https://img.shields.io/pypi/l/faust.svg
    :alt: BSD License
    :target: https://opensource.org/licenses/BSD-3-Clause

.. |wheel| image:: https://img.shields.io/pypi/wheel/faust.svg
    :alt: faust can be installed via wheel
    :target: http://pypi.python.org/pypi/faust/

.. |pyversion| image:: https://img.shields.io/pypi/pyversions/faust.svg
    :alt: Supported Python versions.
    :target: http://pypi.python.org/pypi/faust/

.. |pyimp| image:: https://img.shields.io/pypi/implementation/faust.svg
    :alt: Support Python implementations.
    :target: http://pypi.python.org/pypi/faust/

