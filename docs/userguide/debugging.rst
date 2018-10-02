.. _guide-debugging:

===========================================
 Debugging
===========================================

.. contents::
    :local:
    :depth: 1

.. module:: faust

.. currentmodule:: faust

Debugging with :pypi:`aiomonitor`
=================================

To use the debugging console you first need to install the :pypi:`aiomonitor`
library:

.. sourcecode:: console

    $ pip install aiomonitor

You can also install it as part of a :ref:`bundle <bundles>`:

.. sourcecode:: console

    $ pip install -U faust[debug]


After :pypi:`aiomonitor` is installed you may start the worker with the
:option:`--debug <faust --debug>` option enabled:

.. sourcecode:: console

    $ faust -A myapp --debug worker -l info
    ┌ƒaµS† v0.9.20─────────────────────────────────────────┐
    │ id        │ word-counts                              │
    │ transport │ kafka://localhost:9092                   │
    │ store     │ rocksdb:                                 │
    │ web       │ http://localhost:6066/                   │
    │ log       │ -stderr- (info)                          │
    │ pid       │ 55522                                    │
    │ hostname  │ grainstate.local                         │
    │ platform  │ CPython 3.6.3 (Darwin x86_64)            │
    │ drivers   │ aiokafka=0.3.2 aiohttp=2.3.7             │
    │ datadir   │ /opt/devel/faust/word-counts-data        │
    └───────────┴──────────────────────────────────────────┘
    [2018-01-04 12:41:07,635: INFO]: Starting aiomonitor at 127.0.0.1:50101
    [2018-01-04 12:41:07,637: INFO]: Starting console at 127.0.0.1:50101
    [2018-01-04 12:41:07,638: INFO]: [^Worker]: Starting...
    [2018-03-13 13:41:39,275: INFO]: [^-App]: Starting...
    [2018-01-04 12:41:07,638: INFO]: [^--Web]: Starting...
    [...]

From the log output you can tell that the :pypi:`aiomonitor` console was
started on the local port 50101. If you get a different output,
such as that the port is already taken you can set a custom port
using the :option:`--console-port <faust worker --console-port>`.

Once you have the port number, you can telnet into the console to use it:

.. sourcecode:: console

    $ telnet localhost 50101
    Trying 127.0.0.1...
    Connected to localhost.
    Escape character is '^]'.

    Asyncio Monitor: 38 tasks running
    Type help for commands

    monitor >>>

Type ``help`` and then press enter to see a list of available commands:

.. sourcecode:: console

    monitor >>> help
    Commands:
             ps               : Show task table
             where taskid     : Show stack frames for a task
             cancel taskid    : Cancel an indicated task
             signal signame   : Send a Unix signal
             console          : Switch to async Python REPL
             quit             : Leave the monitor
    monitor >>>

To exit out of the console you can either type `quit` at the ``monitor >>``
prompt. If that is unresponsive you may hit the special telnet escape character
(:kbd:`Ctrl-]`), to drop you into the telnet command console, and from
there you just type `quit` to exit out of the telnet session:

.. sourcecode:: console

    $> telnet localhost 50101
    Trying 127.0.0.1...
    Connected to localhost.
    Escape character is '^]'.

    Asyncio Monitor: 38 tasks running
    Type help for commands
    monitor >>> ^]
    telnet> quit
    Connection closed.
