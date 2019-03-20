.. _apiref:

===============
 API Reference
===============

:Release: |version|
:Date: |today|

Faust
=====

.. toctree::
    :maxdepth: 1

    faust
    faust.auth
    faust.exceptions
    faust.channels
    faust.events
    faust.joins
    faust.streams
    faust.topics
    faust.windows
    faust.worker

App
===

.. toctree::
    :maxdepth: 1

    faust.app
    faust.app.base
    faust.app.router

Agents
======

.. toctree::
    :maxdepth: 1

    faust.agents
    faust.agents.actor
    faust.agents.agent
    faust.agents.manager
    faust.agents.models
    faust.agents.replies

Fixups
======

.. toctree::
    :maxdepth: 1

    faust.fixups
    faust.fixups.base
    faust.fixups.django

Models
======

.. toctree::
    :maxdepth: 1

    faust.models.base
    faust.models.record

Sensors
=======

.. toctree::
    :maxdepth: 1

    faust.sensors
    faust.sensors.base
    faust.sensors.datadog
    faust.sensors.monitor
    faust.sensors.statsd

Serializers
===========

.. toctree::
    :maxdepth: 1

    faust.serializers.codecs
    faust.serializers.registry

Stores
======

.. toctree::
    :maxdepth: 1

    faust.stores
    faust.stores.base
    faust.stores.memory
    faust.stores.rocksdb

Tables
======

.. toctree::
    :maxdepth: 1

    faust.tables
    faust.tables.base
    faust.tables.manager
    faust.tables.objects
    faust.tables.recovery
    faust.tables.sets
    faust.tables.table
    faust.tables.wrappers

Transports
==========

.. toctree::
    :maxdepth: 1

    faust.transport
    faust.transport.base
    faust.transport.conductor
    faust.transport.consumer
    faust.transport.producer
    faust.transport.drivers
    faust.transport.drivers.aiokafka
    faust.transport.drivers.memory
    faust.transport.utils

Assignor
========

.. toctree::
    :maxdepth: 1

    faust.assignor.client_assignment
    faust.assignor.cluster_assignment
    faust.assignor.copartitioned_assignor
    faust.assignor.leader_assignor
    faust.assignor.partition_assignor

Types
=====

.. toctree::
    :maxdepth: 1

    faust.types.agents
    faust.types.app
    faust.types.assignor
    faust.types.auth
    faust.types.channels
    faust.types.codecs
    faust.types.core
    faust.types.enums
    faust.types.events
    faust.types.fixups
    faust.types.joins
    faust.types.models
    faust.types.router
    faust.types.sensors
    faust.types.serializers
    faust.types.settings
    faust.types.stores
    faust.types.streams
    faust.types.tables
    faust.types.topics
    faust.types.transports
    faust.types.tuples
    faust.types.web
    faust.types.windows

Utils
=====

.. toctree::
    :maxdepth: 1

    faust.utils.codegen
    faust.utils.cron
    faust.utils.functional
    faust.utils.iso8601
    faust.utils.json
    faust.utils.platforms
    faust.utils.tracing
    faust.utils.urls
    faust.utils.venusian

Terminal (TTY) Utilities
------------------------

.. toctree::
    :maxdepth: 1

    faust.utils.terminal
    faust.utils.terminal.spinners
    faust.utils.terminal.tables

Web
===

.. toctree::
    :maxdepth: 1

    faust.web.apps.graph
    faust.web.apps.router
    faust.web.apps.stats
    faust.web.base
    faust.web.blueprints
    faust.web.cache
    faust.web.cache.backends
    faust.web.cache.backends.base
    faust.web.cache.backends.memory
    faust.web.cache.backends.redis
    faust.web.cache.cache
    faust.web.cache.exceptions
    faust.web.drivers
    faust.web.drivers.aiohttp
    faust.web.exceptions
    faust.web.views

CLI
===

.. toctree::
    :maxdepth: 1

    faust.cli.agents
    faust.cli.base
    faust.cli.clean_versions
    faust.cli.completion
    faust.cli.faust
    faust.cli.model
    faust.cli.models
    faust.cli.params
    faust.cli.reset
    faust.cli.send
    faust.cli.tables
    faust.cli.worker
