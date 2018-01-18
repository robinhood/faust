=======================================================================
 Faust - Python Stream Processing
=======================================================================

.. topic:: \

    *“I am not omniscient, but I know a lot.”*

    -- Goethe, *Faust: First part*

**Faust** is a Python library for *event processing* and *streaming applications*
that are decentralized and fault-tolerant.

Heavily inspired by `Kafka Streams`_, Faust takes
a radically much more straightforward approach to stream processing
and is very simple to learn and use.

Faust lets you write streaming pipelines using native Python code,
so instead of a DSL like ``stream().groupBy(x).filterNot(y).etc.``, you
use Python asynchronous generators that reuse your existing Python code:

.. sourcecode:: python

    @app.agent()
    async def process(stream):
        async for event in stream:
            if event > 1000:
                ...

It is also similar to tools such as `Apache Spark`_/`Storm`_/`Samza`_,
and `Apache Flink`_, and the Faust API is used for both *stream processing*
and *event processing*, as provided by libraries such as :pypi:`Celery`.

Faust optionally runs a web server so you can host your Web App in
the same system, allowing you to rapidly prototype traditionally complex
web app architectures that are easy to deploy and scale.

**Learn more about Faust in the** :ref:`intro` **introduction page**
    to read more about Faust, system requirements, installation instructions,
    community resources, and more.

**or go directly to the** :ref:`quickstart` **tutorial**
    to see Faust in action by programming a streaming application.

.. _`Kafka Streams`: https://kafka.apache.org/documentation/streams
.. _`Apache Spark`: http://spark.apache.org
.. _`Storm`: http://storm.apache.org
.. _`Samza`: http://samza.apache.org
.. _`Apache Flink`: http://flink.apache.org



Contents
========

.. toctree::
    :maxdepth: 1

    copyright

.. toctree::
    :maxdepth: 1

    introduction

.. toctree::
    :maxdepth: 2

    playbooks/index
    userguide/index

.. toctree::
    :maxdepth: 1

    faq
    reference/index
    changelog
    contributing
    developerguide/index
    glossary

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

