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
    faust.exceptions
    faust.app
    faust.channels
    faust.router
    faust.topics
    faust.windows
    faust.worker

Agents
======

.. toctree::
    :maxdepth: 1

    faust.agents
    faust.agents.agent
    faust.agents.models
    faust.agents.replies

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
    faust.sensors.monitor
    faust.sensors.statsd

Serializers
===========

.. toctree::
    :maxdepth: 1

    faust.serializers.avro
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

Streams
=======

.. toctree::
    :maxdepth: 1

    faust.streams.joins
    faust.streams.stream

Tables
======

.. toctree::
    :maxdepth: 1

    faust.tables
    faust.tables.base
    faust.tables.manager
    faust.tables.set
    faust.tables.table
    faust.tables.wrappers

Transports
==========

.. toctree::
    :maxdepth: 1

    faust.transport
    faust.transport.aiokafka
    faust.transport.base

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
    faust.types.channels
    faust.types.codecs
    faust.types.core
    faust.types.joins
    faust.types.models
    faust.types.router
    faust.types.sensors
    faust.types.serializers
    faust.types.stores
    faust.types.streams
    faust.types.tables
    faust.types.topics
    faust.types.windows
    faust.types.transports
    faust.types.tuples

Utils
=====

.. toctree::
    :maxdepth: 1

    faust.utils.aiolocals
    faust.utils.aiter
    faust.utils.avro.serializers
    faust.utils.avro.servers
    faust.utils.collections
    faust.utils.compat
    faust.utils.functional
    faust.utils.futures
    faust.utils.imports
    faust.utils.iso8601
    faust.utils.json
    faust.utils.objects
    faust.utils.platforms
    faust.utils.text

Web
===

.. toctree::
    :maxdepth: 1

    faust.web.apps.graph
    faust.web.apps.router
    faust.web.apps.stats
    faust.web.base
    faust.web.drivers
    faust.web.drivers.aiohttp
    faust.web.site
    faust.web.views

CLI
===

.. toctree::
    :maxdepth: 1

    faust.cli.agents
    faust.cli.base
    faust.cli.faust
    faust.cli.model
    faust.cli.models
    faust.cli.reset
    faust.cli.send
    faust.cli.tables
    faust.cli.worker
