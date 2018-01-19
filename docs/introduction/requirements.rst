==================
 What do I need?
==================

.. contents::
    :local:
    :depth: 1

.. sidebar:: Version Requirements
    :subtitle: Faust version 1.0 runs on

    **Core**

    - Python 3.6 or later.
    - Kafka 0.10.1 or later.

    **Extensions**

    - RocksDB 5.0 or later, python-rocksdb

Faust requires Python 3.6 or later, and a running Kafka broker.

There's currently no plan to port Faust to earlier Python versions;
please get in touch if you want to work on this.

.. admonition:: RocksDB On MacOS Sierra

    To install :pypi:`python-rocksdb` on MacOS Sierra you need to
    specify some additional compiler flags:

    .. sourcecode:: console

        $ CFLAGS='-std=c++11 -stdlib=libc++ -mmacosx-version-min=10.10' \
            pip install -U --no-cache python-rocksdb

Extensions
==========

+--------------+-------------+--------------------------------------------------+
| **Name**     | **Version** | **Bundle**                                       |
+--------------+-------------+--------------------------------------------------+
| rocksdb      | 5.0         | ``pip install faust[rocksdb]``                   |
+--------------+-------------+--------------------------------------------------+
| statsd       | 3.2.1       | ``pip install faust[statsd]``                    |
+--------------+-------------+--------------------------------------------------+
| uvloop       | 0.8.1       | ``pip install faust[uvloop]``                    |
+--------------+-------------+--------------------------------------------------+
| aiodns       | 1.1         | ``pip install faust[fast]``                      |
+--------------+-------------+--------------------------------------------------+
| setproctitle | 1.1         | ``pip install faust[setproctitle]`` (also debug) |
+--------------+-------------+--------------------------------------------------+
| aiomonitor   | 0.3         | ``pip install faust[debug]``                     |
+--------------+-------------+--------------------------------------------------+

.. note::

    See bundles in the :ref:`installation` instructions section of
    this document for a list of supported :pypi:`setuptools` extensions.

.. admonition:: To specify multiple extensions at the same time

    separate extensions with the comma:

    .. sourcecode:: console

        $ pip install fuast[uvloop,fast,rocksdb,statsd]

