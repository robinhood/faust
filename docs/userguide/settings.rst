.. _guide-settings:

====================================
 Configuration Reference
====================================

.. module:: faust
    :noindex:

.. currentmodule:: faust

.. contents::
    :local:
    :depth: 2

.. _settings-required:

Required Settings
=================

.. setting:: id

``id``
------

:type: ``str``

A string uniquely identifying the app, shared across all
instances such that two app instances with the same `id` are
considered to be in the same "group".

This parameter is required.

.. admonition:: The id and Kafka

    When using Kafka, the id is used to generate app-local topics, and
    names for consumer groups.

.. include:: ../includes/settingref.txt

