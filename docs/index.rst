=======================================================================
 Faust - Python Stream Processing
=======================================================================

.. topic:: \

    *“I am not omniscient, but I know a lot.”*

    -- Goethe, *Faust: First part*

**Faust** is a Python library for *event processing* and *streaming applications*
that are decentralized and fault-tolerant.

It is heavily inspired by tools such as `Kafka Streams`_, `Apache Spark`_,
`Apache Storm`_, `Apache Samza`_ and `Apache Flink`_; but takes
a radically much simpler approach to stream processing.

Faust defines a simple API that can be used for both *stream processing*
(e.g. `Kafka Streams`_), and *event processing* (as provided by libraries such as
:pypi:`celery`).  It also runs a web server so you can optionally host your web
app directly from Faust. This enables you to rapidly prototype traditionally
complicated web app architectures, in a simple manner that is also easy
to deploy and maintain.

To learn more about faust go to the :ref:`intro` introduction page,
or go directly to the :ref:`quickstart` tutorial.

.. _`Kafka Streams`: https://kafka.apache.org/documentation/streams
.. _`Apache Spark`: http://spark.apache.org
.. _`Apache Storm`: http://storm.apache.org
.. _`Apache Flink`: http://flink.apache.org
.. _`Apache Samza`: http://samza.apache.org



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

