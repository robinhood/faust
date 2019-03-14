.. guide-cli:

========================
 Command-line Interface
========================

.. contents::
    :local:
    :depth: 2

.. module:: faust

.. currentmodule:: faust

.. program:: faust

Program: ``faust``
==================

The :program:`faust` umbrella command hosts all command-line functionality
for Faust. Projects may add custom commands using the ``@app.command``
decorator (see :ref:`tasks-cli-commands`).

**Options:**

.. cmdoption:: -A, --app

    Path of Faust application to use, or the name of a module.

.. cmdoption:: --quiet, --no-quiet, -q

    Silence output to <stdout>/<stderr>.

.. cmdoption:: --debug, --no-debug

    Enable debugging output, and the blocking detector.

.. cmdoption:: --workdir, -W

    Working directory to change to after start.

.. cmdoption:: --datadir

    Directory to keep application state.

.. cmdoption:: --json

    Return output in machine-readable JSON format.

.. cmdoption:: --loop, -L

    Event loop implementation to use: aio (default), gevent, uvloop.

.. admonition:: Why is ``examples/word_count.py`` used as the program?

    The convention for Faust projects is to define an entrypoint for
    the Faust command using ``app.main()`` - see :ref:`application-main`
    to see how to do so.

    For a standalone program such as ``examples/word_count.py`` this
    is accomplished by adding the following at the end of the file:

    .. sourcecode:: python

        if __name__ == '__main__':
            app.main()

    For a project organized in modules (a package) you can add a
    ``package/__main__.py`` module:

    .. sourcecode:: python

        # package/__main__.py
        from package.app import app
        app.main()

    Or use setuptools entrypoints so that ``pip install myproj`` installs
    a command-line program.

    Even if you don't add an entrypoint you can always use the
    :program:`faust` program by specifying the path to an app.

    Either the name of a module having an ``app`` attribute:

    .. sourcecode:: console

        $ faust -A examples.word_count

    or specifying the attribute directly:

    .. sourcecode:: console

        $ faust -A examples.word_count:app

``faust --version`` - Show version information and exit.
--------------------------------------------------------

Example:

.. sourcecode:: console

    $ python examples/word_count.py --version
    word_count.py, version Faust 0.9.39

``faust --help`` - Show help and exit.
--------------------------------------

Example:

.. sourcecode:: console

    $ python examples/word_count.py --help
    Usage: word_count.py [OPTIONS] COMMAND [ARGS]...

    Faust command-line interface.

    Options:
    -L, --loop [aio|gevent|eventlet|uvloop]
                                    Event loop implementation to use.
    --json / --no-json              Prefer data to be emitted in json format.
    -D, --datadir DIRECTORY         Directory to keep application state.
    -W, --workdir DIRECTORY         Working directory to change to after start.
    --no-color / --color            Enable colors in output.
    --debug / --no-debug            Enable debugging output, and the blocking
                                    detector.
    -q, --quiet / --no-quiet        Silence output to <stdout>/<stderr>.
    -A, --app TEXT                  Path of Faust application to use, or the
                                    name of a module.
    --version                       Show the version and exit.
    --help                          Show this message and exit.

    Commands:
    agents  List agents.
    model   Show model detail.
    models  List all available models as tabulated list.
    reset   Delete local table state.
    send    Send message to agent/topic.
    tables  List available tables.
    worker  Start ƒaust worker instance.

.. program:: faust agents

``faust agents`` - List agents defined in this application.
-----------------------------------------------------------

Example:

.. sourcecode:: console

    $ python examples/word_count.py agents
    ┌Agents──────────┬─────────────────────────────────────────────┬──────────────────────────────────────────┐
    │ name           │ topic                                       │ help                                     │
    ├────────────────┼─────────────────────────────────────────────┼──────────────────────────────────────────┤
    │ @count_words   │ word-counts-examples.word_count.count_words │ Count words from blog post article body. │
    │ @shuffle_words │ posts                                       │ <N/A>                                    │
    └────────────────┴─────────────────────────────────────────────┴──────────────────────────────────────────┘

JSON Output using ``--json``:

.. sourcecode:: console

    $ python examples/word_count.py --json agents
    [{"name": "@count_words",
      "topic": "word-counts-examples.word_count.count_words",
      "help": "Count words from blog post article body."},
     {"name": "@shuffle_words",
      "topic": "posts",
      "help": "<N/A>"}]

.. program:: faust models

``faust models`` - List defined serialization models.
-----------------------------------------------------

Example:

.. sourcecode:: console

    $ python examples/word_count.py models
    ┌Models┬───────┐
    │ name │ help  │
    ├──────┼───────┤
    │ Word │ <N/A> │
    └──────┴───────┘

JSON Output using ``--json``:

.. sourcecode:: console

    python examples/word_count.py --json models
    [{"name": "Word", "help": "<N/A>"}]

.. program:: faust model

``faust model <name>`` - List model fields by model name.
---------------------------------------------------------

Example:

.. sourcecode:: console

    $ python examples/word_count.py model Word
    ┌Word───┬──────┬──────────┐
    │ field │ type │ default* │
    ├───────┼──────┼──────────┤
    │ word  │ str  │ *        │
    └───────┴──────┴──────────┘

JSON Output using ``--json``:

.. sourcecode:: console

    $ python examples/word_count.py --json model Word
    [{"field": "word", "type": "str", "default*": "*"}]

.. program:: faust reset

``faust reset`` - Delete local table state.
-------------------------------------------

.. warning::

    This command will result in the destruction of the following files:

        1) The local database directories/files backing tables
            (does not apply if an in-memory store like memory:// is used).

.. note::

    This data is technically recoverable from the Kafka cluster (if
    intact), but it'll take a long time to get the data back as
    you need to consume each changelog topic in total.

    It'd be faster to copy the data from any standbys that happen
    to have the topic partitions you require.

Example:

.. sourcecode:: console

    $ python examples/word_count.py reset

.. program:: faust send

``faust send <topic/agent> <message_value>`` - Send message.
------------------------------------------------------------

**Options:**

.. cmdoption:: --key-type, -K

    Name of model to serialize key into.

.. cmdoption:: --key-serializer

    Override default serializer for key.

.. cmdoption:: --value-type, -V

    Name of model to serialize value into.

.. cmdoption:: --value-serializer

    Override default serializer for value.

.. cmdoption:: --key, -k

    String value for key (use json if model).

.. cmdoption:: --partition

    Specific partition to send to.

.. cmdoption:: --repeat, -r

    Send message n times.

.. cmdoption:: --min-latency

    Minimum delay between sending.

.. cmdoption:: --max-latency

    Maximum delay between sending.

**Examples:**

Send to agent by name using ``@`` prefix:

.. sourcecode:: console

    $ python examples/word_count.py send @word_count "foo"

Send to topic by name (no prefix):

.. sourcecode:: console

    $ python examples/word_count.py send mytopic "foo"
    {"topic": "mytopic",
     "partition": 2,
     "topic_partition": ["mytopic", 2],
     "offset": 0,
     "timestamp": 1520974493620,
     "timestamp_type": 0}

To get help:

.. sourcecode:: console

    $ python examples/word_count.py send --help
    Usage: word_count.py send [OPTIONS] ENTITY [VALUE]

    Send message to agent/topic.

    Options:
    -K, --key-type TEXT      Name of model to serialize key into.
    --key-serializer TEXT    Override default serializer for key.
    -V, --value-type TEXT    Name of model to serialize value into.
    --value-serializer TEXT  Override default serializer for value.
    -k, --key TEXT           String value for key (use json if model).
    --partition INTEGER      Specific partition to send to.
    -r, --repeat INTEGER     Send message n times.
    --min-latency FLOAT      Minimum delay between sending.
    --max-latency FLOAT      Maximum delay between sending.
    --help                   Show this message and exit.

.. program:: faust tables

``faust tables`` - List Tables (distributed K/V stores).
--------------------------------------------------------

Example:

.. sourcecode:: console

    $ python examples/word_count.py tables
    ┌Tables───────┬───────────────────────────────────┐
    │ name        │ help                              │
    ├─────────────┼───────────────────────────────────┤
    │ word_counts │ Keep count of words (str to int). │
    └─────────────┴───────────────────────────────────┘

JSON Output using ``--json``:

.. sourcecode:: console

    $ python examples/word_count.py --json tables
    [{"name": "word_counts", "help": "Keep count of words (str to int)."}]

.. program:: faust worker

``faust worker`` - Start Faust worker instance.
-----------------------------------------------

A "worker" starts a single instance of a Faust application.

**Options:**

.. cmdoption:: --logfile, -f

    Path to logfile (default is <stderr>).

.. cmdoption:: --loglevel, -l

    Logging level to use: ``CRIT|ERROR|WARN|INFO|DEBUG``.

.. cmdoption:: --blocking-timeout

    Blocking detector timeout (requires --debug).

.. cmdoption:: --without-web

    Do not start embedded web server.

.. cmdoption:: --web-host, -h

    Canonical host name for the web server.

.. cmdoption:: --web-port, -p

    Port to run web server on (default is 6066).

.. cmdoption:: --web-bind, -b

    Network mask to bind web server to (default is "0.0.0.0" - all
    interfaces).

.. cmdoption:: --console-port

    When :option:`faust --debug` is enabled this specifies the port
    to run the :pypi:`aiomonitor` console on (default is 50101).

**Examples:**

.. sourcecode:: console

    $ python examples/word_count.py worker
    ┌ƒaµS† v1.0.0──────────────────────────────────────────┐
    │ id        │ word-counts                              │
    │ transport │ kafka://localhost:9092                   │
    │ store     │ rocksdb:                                 │
    │ web       │ http://localhost:6066/                   │
    │ log       │ -stderr- (warn)                          │
    │ pid       │ 46052                                    │
    │ hostname  │ grainstate.local                         │
    │ platform  │ CPython 3.6.4 (Darwin x86_64)            │
    │ drivers   │ aiokafka=0.4.0 aiohttp=3.0.8             │
    │ datadir   │ /opt/devel/faust/word-counts-data        │
    │ appdir    │ /opt/devel/faust/word-counts-data/v1     │
    └───────────┴──────────────────────────────────────────┘
    starting➢ 😊

To get more logging use `-l info` (or further `-l debug`):

.. sourcecode:: console

    $ python examples/word_count.py worker -l info
    ┌ƒaµS† v1.0.0──────────────────────────────────────────┐
    │ id        │ word-counts                              │
    │ transport │ kafka://localhost:9092                   │
    │ store     │ rocksdb:                                 │
    │ web       │ http://localhost:6066/                   │
    │ log       │ -stderr- (info)                          │
    │ pid       │ 46034                                    │
    │ hostname  │ grainstate.local                         │
    │ platform  │ CPython 3.6.4 (Darwin x86_64)            │
    │ drivers   │ aiokafka=0.4.0 aiohttp=3.0.8             │
    │ datadir   │ /opt/devel/faust/word-counts-data        │
    │ appdir    │ /opt/devel/faust/word-counts-data/v1     │
    └───────────┴──────────────────────────────────────────┘
    starting^[2018-03-13 13:41:39,269: INFO]: [^Worker]: Starting...
    [2018-03-13 13:41:39,275: INFO]: [^-App]: Starting...
    [2018-03-13 13:41:39,271: INFO]: [^--Web]: Starting...
    [2018-03-13 13:41:39,272: INFO]: [^---ServerThread]: Starting...
    [2018-03-13 13:41:39,273: INFO]: [^--Web]: Serving on http://localhost:6066/
    [2018-03-13 13:41:39,275: INFO]: [^--Monitor]: Starting...
    [2018-03-13 13:41:39,275: INFO]: [^--Producer]: Starting...
    [2018-03-13 13:41:39,317: INFO]: [^--Consumer]: Starting...
    [2018-03-13 13:41:39,325: INFO]: [^--LeaderAssignor]: Starting...
    [2018-03-13 13:41:39,325: INFO]: [^--Producer]: Creating topic word-counts-__assignor-__leader
    [2018-03-13 13:41:39,325: INFO]: [^--Producer]: Nodes: [0]
    [2018-03-13 13:41:39,668: INFO]: [^--Producer]: Topic word-counts-__assignor-__leader created.
    [2018-03-13 13:41:39,669: INFO]: [^--ReplyConsumer]: Starting...
    [2018-03-13 13:41:39,669: INFO]: [^--Agent]: Starting...
    [2018-03-13 13:41:39,673: INFO]: [^---OneForOneSupervisor]: Starting...
    [2018-03-13 13:41:39,673: INFO]: [^---Agent*: examples.word_co[.]shuffle_words]: Starting...
    [2018-03-13 13:41:39,673: INFO]: [^--Agent]: Starting...
    [2018-03-13 13:41:39,674: INFO]: [^---OneForOneSupervisor]: Starting...
    [2018-03-13 13:41:39,674: INFO]: [^---Agent*: examples.word_count.count_words]: Starting...
    [2018-03-13 13:41:39,674: INFO]: [^--Conductor]: Starting...
    [2018-03-13 13:41:39,674: INFO]: [^--TableManager]: Starting...
    [2018-03-13 13:41:39,675: INFO]: [^--Stream: <(*)Topic: posts@0x10497e5f8>]: Starting...
    [2018-03-13 13:41:39,675: INFO]: [^--Stream: <(*)Topic: wo...s@0x105f73b38>]: Starting...
    [...]

To get help use ``faust worker --help``:

.. sourcecode:: console

    $ python examples/word_count.py worker --help
    Usage: word_count.py worker [OPTIONS]

    Start ƒaust worker instance.

    Options:
    -f, --logfile PATH              Path to logfile (default is <stderr>).
    -l, --loglevel [crit|error|warn|info|debug]
                                    Logging level to use.
    --blocking-timeout FLOAT        Blocking detector timeout (requires
                                    --debug).
    -p, --web-port RANGE[1-65535]   Port to run web server on.
    -b, --web-bind TEXT
    -h, --web-host TEXT             Canonical host name for the web server.
    --console-port RANGE[1-65535]   (when --debug:) Port to run debugger console
                                    on.
    --help                          Show this message and exit.
