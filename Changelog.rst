.. _changelog:

==============================
 Change history for Faust 1.5
==============================

This document contain change notes for bugfix releases in
the Faust 1.5 series. If you're looking for previous releases,
please visit the :ref:`history` section.

.. _version-1.5.0:

1.5.0
=====
:release-date: TBA
:release-by: TBA

- **App**: New :setting:`consumer_auto_offset_reset` setting.

    Contributed by Ryan Whitten (:github_user:`rwhitten577`).

- **App**: Autodiscovery now ignores modules matching "*test*" (Issue #242).

    Contributed by Chris Seto (:github_user:`chrisseto`).

- **Stream**: Fixed deadlock when using ``Stream.take`` to buffer events
  (Issue #262).

    Contributed by Nimi Wariboko Jr (:github_user:`nemosupremo`).
