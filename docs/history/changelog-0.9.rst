.. _changelog-0.9:

==============================
 Change history for Faust 0.9
==============================

This document contain historical change notes for bugfix releases in
the Faust 0.x series. To see the most recent changelog
please visit :ref:`changelog`.

.. contents::
    :local:
    :depth: 1

.. _version-0.9.65:

0.9.65
======
:release-date: 2018-04-27 2:04 P.M PDT
:release-by: Vineet Goel

- **Producer**: New setting to configure compression.

  + See :setting:`producer_compression_type`.

- **Documentation**: New :ref:`settings-producer` section.

.. _version-0.9.64:

0.9.64
======
:release-date: 2018-04-26 4:48 P.M PDT
:release-by: Ask Solem

- **Models**: Optimization for ``FieldDescriptor.__get__``.

- **Serialization**: Optimization for :mod:`faust.utils.json`.

.. _version-0.9.63:

0.9.63
======
:release-date: 2018-04-26 04:32 P.M PDT
:release-by: Vineet Goel

- **Requirements**:

    + Now depends on :pypi:`aiokafka` 0.4.5 (Robinhood fork).

- **Models**: ``Record.asdict()`` and ``to_representation()`` were slow
  on complicated models, so we are now using code generation to optimize them.

    .. warning::

        You are no longer allowed to override ``Record.asdict()``.

.. _version-0.9.62:

0.9.62
======
:release-date: 2018-04-26 12:06 P.M PDT
:release-by: Ask Solem

- **Requirements**:

    + Now depends on :ref:`Mode 1.12.2 <mode:version-1.12.2>`.

    + Now depends on :pypi:`aiokafka` 0.4.4 (Robinhood fork).

- **Consumer**: Fixed :exc:`asyncio.base_futures.IllegalStateError` error
  in commit handler.

- **CLI**: Fixed bug when invoking worker using ``faust -A``.
