===============================
 Design considerations
===============================

.. contents::
    :local:
    :depth: 1

Modern Python
    Faust uses current Python 3 features such as
    :keyword:`async <async def>`/:keyword:`await` and type annotations. It's statically
    typed and verified by the `mypy`_ type checker. You can take advantage of
    type annotations when writing Faust applications, but
    this is not mandatory.

Library
    Faust is designed to be used as a library, and embeds into
    any existing Python program, while also including helpers that
    make it easy to deploy applications without boilerplate.

Live happily, die quickly
    Faust is programmed to crash on encountering an error such as losing
    the connection to Kafka.  That means error recovery is up to supervisor
    tools such as `supervisord`_, `Circus`_, or one provided by your Operating
    System.

Extensible
    Faust abstracts away storages, serializers, and even message transports,
    to make it easy for developers to extend Faust with new capabilities,
    and integrate into your existing systems.

Lean
    The source code is short and readable and serves as a good starting point
    for anyone who wants to learn how Kafka stream processing systems work.

.. _`mypy`: http://mypy-lang.org

.. _`supervisord`: http://supervisord.org

.. _`circus`: http://circus.readthedocs.io/
