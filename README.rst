=====================================================================
 Stream processing
=====================================================================

|build-status| |coverage| |license| |wheel| |pyversion| |pyimp|

:Version: 1.0.0
:Web: http://faust.readthedocs.org/
:Download: http://pypi.python.org/pypi/faust
:Source: http://github.com/robinhoodmarkets/faust
:Keywords: KEYWORDS

About
=====

# I'm too lazy to edit the defaults.

.. _faq:

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

.. _installation:

Installation
============

You can install faust either via the Python Package Index (PyPI)
or from source.

To install using `pip`,::

    $ pip install -U faust

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

    $ pip install https://github.com/robinhoodmarkets/faust/zipball/master#egg=faust

.. |build-status| image:: https://secure.travis-ci.org/robinhoodmarkets/faust.png?branch=master
    :alt: Build status
    :target: https://travis-ci.org/robinhoodmarkets/faust

.. |coverage| image:: https://codecov.io/github/robinhoodmarkets/faust/coverage.svg?branch=master
    :target: https://codecov.io/github/robinhoodmarkets/faust?branch=master

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

