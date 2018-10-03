.. _changelog:

==============================
 Change history for Faust 1.2
==============================

This document contain change notes for bugfix releases in
the Faust 1.2 series. If you're looking for previous releases,
please visit the :ref:`history` section.

1.2.0
=====
:release-date: TBA
:release-by: TBA

.. _v120-news:

News
----

- **Requirements**

    + Now depends on :ref:`Mode 1.18.0 <mode:version-1.18.0>`.

- **Web**: Users can now disable the web server from the faust worker
    (Issue #167).

    Either by passing :option:`faust worker --without-web` on the
    command-line, or by using the new :setting:`web_enable` setting.

- **App**: Channels and topics now take default
    ``key_serializer``/``value_serializer`` from ``key_type``/``value_type``
    when they are specified as models (Issue #173).

        This ensures support for custom codecs specified using
        the model ``serializer`` class keyword::

            class X(faust.Record, serializer='msgpack'):
                x: int
                y: int

- **CLI**: Command-line improvements.

    - All subcommands are now executed by :class:`mode.Worker`.

        This means all commands will have the same environment set up,
        including logging, signal handling, blocking detection support,
        and remote :pypi:`aiomonitor` console support.

    - ``faust worker`` options moved to top level (built-in) options:

        + ``--logfile``
        + ``--loglevel``
        + ``--console-port``
        + ``--blocking-timeout``

        To be backwards compatible these options can now appear
        before and after the ``faust worker`` command on the command-line
        (but for all other commands they need to be specified before
        the command name):

        .. sourcecode:: console

            $ ./examples/withdrawals.py -l info worker  # OK

            $ ./examples/withdrawals.py worker -l info  # OK

            $ ./examples/withdrawals.py -l info agents  # OK

            $ ./examples/withdrawals.py agents -l info  # ERROR!

    - If you want a long running background command that will run
      even after returning, use: ``daemon=True``.

        If enabled the program will not shut down until either the
        user hits :kbd:`Control-c`, or the process is terminated by a signal:

        .. code-block:: python

            @app.command(daemon=True)
            async def foo():
                print('starting')
                # set up stuff
                return  # command will continue to run after return.

    - Web driver preference can now be specified using the :setting:`web`
      setting (default is ``aiohttp://``).

- **CLI**: New :func:`~faust.cli.faust.call_command` utility for testing.

    This can be used to safely call a command by name, given an argument list.

- **Web**: Blueprints can now be added to apps by using strings

    Example:

    .. sourcecode:: python

        app = faust.App('name')

        app.web.blueprints.add('/users/', 'proj.users.views:blueprint')

- **Web**: Refactored ``faust.web`` to be decoupled from the faust App.

    - The web server is now started by :class:`faust.App`, not
      :class:`faust.Worker`.

    - This means web related functionality is always available from
      ``app.web`` attribute.

        After registering a blueprint, you can get the URL for a view by
        name simply by calling: ``app.web.url_for('user:detail', user_id=3)``.

    - Completely removed the ``Website`` class, everything is now in the
      ``faust.web.Web`` class and individual web framework drivers.

    - Views can now define a ``__post_init__`` method as supported by
      dataclasses/Faust models.

    - The aiohttp ``json()`` response method now uses the Faust json
      serializer for automatic support of ``__json__`` callbacks.

- **Web**: New cache decorator and cache backends

    The cache decorator can be used to cache views, supporting
    both in-memory and Redis for storing the cache.

    .. sourcecode:: python

        from faust import web

        blueprint = web.Blueprint('users')
        cache = blueprint.cache(timeout=300.0)

        @blueprint.route('/', name='list')
        class UserListView(web.View):

            @cache.view()
            async def get(self, request: web.Request) -> web.Response:
                return web.json(...)

        @blueprint.route('/{user_id}/', name='detail')
        class UserDetailView(web.View):

            @cache.view(timeout=10.0)
            async def get(self,
                          request: web.Request,
                          user_id: str) -> web.Response:
                return web.json(...)

    At this point the views are realized and can be used
    from Python code, but the cached ``get`` method handlers
    cannot be called yet.

    To actually use the view from a web server, we need to register
    the blueprint to an app:

    .. sourcecode:: python

        app = faust.App(
            'name',
            broker='kafka://',
            cache='redis://',
        )
        app.web.blueprints.add('/user/', 'where.is:user_blueprint')

    After this the web server will have fully-realized views
    with actually cached method handlers.

    The blueprint is registered with a prefix, so the URL for the
    ``UserListView`` is now ``/user/``, and the URL for the ``UserDetailView``
    is ``/user/{user_id}/``.

- **Distribution**: Setuptools metadata now moved to ``setup.py`` to
                    keep in one location.

    This also helps the README banner icons show the correct information.

    Contributed by Bryant Biggs (:github_user:`bryantbiggs`)

- Documentation and examples improvements by

    + Denis Kataev (:github_user:`kataev`).
