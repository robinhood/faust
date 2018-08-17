.. _changelog:

==============================
 Change history for Faust 1.1
==============================

This document contain change notes for bugfix releases in
the Faust 1.1 series. If you're looking for previous releases,
please visit the :ref:`history` section.

.. _version-1.1.0:

1.1.0
=====
:release-date: TBA
:release-by:

.. _v110-important-notes:

Important Notes
---------------

    - **API**: Agent/Channel.send now requires keyword-only arguments only

        Users often make the mistake of doing:

        .. sourcecode:: python

            channel.send(x)

        and expect that to send ``x`` as the value.

        But the signature is ``(key, value, ...)``, so it ends up being
        ``channel.send(key=x, value=None)``.

        Fixing this will come in two parts:

        1) Faust 1.1 (this change): Make them keyword-only arguments

            This will make it an error if the names of arguments are not
            specified:

            .. sourcecode:: python

                channel.send(key, value)

            Needs to be changed to:

            .. sourcecode:: python

                channel.send(key=key, value=value)

        2) Faust 1.2: We will change the signature
           to ``channel.send(value, key=key, ...)``

            At this stage all existing code will have changed to using
            keyword-only arguments.

- **App**: The default key serializer is now ``raw`` (Issue #142).

    The default *value* serializer will still be ``json``, but for keys
    it does not make as much sense to use json as the default: keys are very
    rarely expressed using complex structures.

    If you depend on the Faust 1.0 behavior you should override the
    default key serializer for the app:

    .. sourcecode:: python

        app = faust.App('myapp', ..., key_serializer='json')

    Contributed by Allison Wang.

.. _v110-news:

News
----

- **App**: ``@app.task`` decorator now accepts ``on_leader``
           argument (Issue #131).

    Tasks created using the ``@app.task`` decorator will run once a worker
    is fully started.

    Similar to the ``@app.timer`` decorator, you can now create one-shot tasks
    that run on the leader worker only:

    .. sourcecode:: python

        @app.task(on_leader=True)
        async def mytask():
            print('WORKER STARTED, AND I AM THE LEADER')

    The decorated function may also accept the ``app`` as an argument:

    .. sourcecode:: python

        @app.task(on_leader=True)
        async def mytask(app):
            print(f'WORKER FOR APP {app} STARTED, AND I AM THE LEADER')

- **App**: New ``app.producer_only`` attribute.

    If set the worker will start the app without
    consumer/tables/agents/topics.
