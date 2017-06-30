.. _guide-services:

==================
 Services
==================

.. module:: faust.utils.services

.. currentmodule:: faust.utils.services

.. contents::
    :local:
    :depth: 1

Basics
======

The Service class manages the services and background tasks started
by the async program, so that we can implement graceful shutdown
and also helps us visualize the relationships between
services in a dependency graph.

Anything internally in Faust that can be started/stopped and restarted
is probably a subclass of the :class:`Service` class:
App/Worker/Stream/Actor/TopicManager/TableManager, etc.

If you want to understand the Faust code base, or extend it with
new services you should read the following guide.

The Service API
===============

A service can be started, and it may start other services
and background tasks.  Most actions in a service are asynchronous, so needs
to be executed from within an async function.

This first section defines the public service API, as if used by the user,
the next section will define the methods service authors write to define new
services.

Methods
-------

.. class:: Service
    :noindex:

    .. automethod:: start
        :noindex:

    .. automethod:: maybe_start
        :noindex:

    .. automethod:: stop
        :noindex:

    .. automethod:: restart
        :noindex:

    .. automethod:: wait_until_stopped
        :noindex:

    .. automethod:: set_shutdown
        :noindex:

Attributes
----------

.. class:: Service
    :noindex:

    .. autoattribute:: started
        :noindex:

    .. autoattribute:: label
        :noindex:

    .. autoattribute:: shortlabel
        :noindex:

    .. autoattribute:: beacon
        :noindex:

Defining new services
=====================

Adding child services
---------------------

Child services can be added in three ways,

1) Using ``add_dependency()`` in ``on_init``:

    .. sourcecode:: python

        class MyService(Service):

            def on_init(self) -> None:
                self.add_dependency(OtherService())

2) Using ``add_dependency()`` in ``on_start``:

    .. sourcecode:: python

        class MyService(Service):

            async def on_start(self) -> None:
                self.add_dependency(OtherService())

3) Using ``on_init_dependencies()``

    This is is a method that if customized should return an iterable
    of service instances:

    .. sourcecode:: python

        from typing import Iterable
        from faust.utils.services import Service, ServiceT

        class MyService(Service):

            def on_init_dependencies(self) -> Iterable[ServiceT]:
                return [ServiceA(), ServiceB()]

Ordering
--------

Knowing exactly what is called, when it's called and in what order
is important, and this table will help you understand that:

Order at start (``await Service.start()``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. The ``on_first_start`` callback is called.
2. Service logs: ``"[Service] Starting..."``.
3. ``on_start`` callback is called.
4. All ``@Service.task`` background tasks are started (in definition order).
5. All child services added by ``add_dependency()``, or
   ``on_init_dependencies())`` are started.
6. Service logs: ``"[Service] Started"``.
7. The ``on_started`` callback is called.

Order when stopping (``await Service.stop()``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Service logs; ``"[Service] Stopping..."``.
2. The ``on_stop()`` callback is called.
3. All child services are stopped, in reverse order.
4. All asyncio futures added by ``add_future()`` are cancelled
   in reverse order.
5. Service logs: ``"[Service] Stopped"``.
6. If ``Service.wait_for_shutdown = True``, it will wait for the
   ``Service.set_shutdown()`` signal to be called.
7. All futures started by ``add_future()`` will be gathered (awaited).
8. The ``on_shutdown()`` callback is called.
9. The service logs: ``"[Service] Shutdown complete!"``.

Order when restarting (``await Service.restart()``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. The service is stopped (``await service.stop()``).
2. The ``on_init()`` callback is called again.
3. The service is started (``await service.start()``).

Callbacks
---------

.. class:: Service
    :noindex:

    .. automethod:: on_start

    .. automethod:: on_first_start

    .. automethod:: on_started

    .. automethod:: on_stop

    .. automethod:: on_shutdown

    .. automethod:: on_restart

Handling Errors
---------------

.. class:: Service

    .. automethod:: crash

Utilities
---------

.. class:: Service
    :noindex:

    .. automethod:: sleep

    .. automethod:: wait

Logging
-------

Your service may add logging to notify the user what is going on, and the
Service class includes some shortcuts to include the service name etc. in
logs.

The ``self.log`` delegate contains shortcuts for logging:

.. sourcecode:: python

    # examples/logging.py

    from faust.utils.logging import get_logger
    from faust.utils.services import Service

    logger = get_logger(__name__)

    class MyService(Service):
        logger = logger

        async def on_start(self) -> None:
            self.log.debug('This is a debug message')
            self.log.info('This is a info message')
            self.log.warn('This is a warning message')
            self.log.error('This is a error message')
            self.log.exception('This is a error message with traceback')
            self.log.crit('This is a critical message')

            self.log.debug('I can also include templates: %r %d %s',
                           [1, 2, 3], 303, 'string')
