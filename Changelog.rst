.. _changelog:

==============================
 Changes
==============================

This document contain change notes for bugfix releases in
the Faust 1.10 series. If you're looking for previous releases,
please visit the :ref:`history` section.

.. _version-1.11.0:

1.11.0
======
:release-date: TBA
:release-by: TBA

- **Requirements**

  + Now depends on :ref:`Mode 4.3.2 <mode:version-4.3.2>`

  + Now depends on :pypi:`robinhood-aiokafka` 1.1.5

- **Tables**: Table HTTP router now forwards HTTP status code
  in responses.

    Contributed by Wjatscheslaw Kewlin :github_user:`slawak`.

- **Tables**: Windowed tables may now have asynchronous ``on_window_close``
  callbacks.

    Contributed by Ramkumar M (:github_user:`billaram`).

.. _version-1.10.4:
