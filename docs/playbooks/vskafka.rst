.. _kafka-streams-comparison:

==================================
 Overview: Faust vs Kafka Streams
==================================

.. contents::
    :local:
    :depth: 1

KStream
=======

- ``.filter()``
- ``.filterNot()``

    Just use the `if` statement:

    .. sourcecode:: python

        @app.agent(topic)
        async def process(stream):
            async for event in stream:
                if event.amount >= 300.0:
                    yield event

- ``.map()``

    Just call the function you want from within the
    :keyword:`async for` iteration:

    .. sourcecode:: python

        @app.agent(Topic)
        async def process(stream):
            async for key, event in stream.items():
                yield myfun(key, event)

- ``.forEach()``

    In KS ``forEach`` is the same as ``map``, but ends the processing chain.

- ``.peek()``

    In KS ``peek`` is the same as ``map``, but documents that the
    action may have a side effect.

- ``.mapValues()``:

    .. sourcecode:: python


        @app.agent(topic)
        async def process(stream):
            async for event in stream:
                yield myfun(event)

- ``.print()``:

    .. sourcecode:: python

        @app.agent(topic)
        async def process(stream):
            async for event in stream:
                print(event)

- ``.writeAsText()``:

    .. sourcecode:: python

        @app.agent(topic)
        async def process(stream):
            async for key, event in stream.items():
                with open(path, 'a') as f:
                    f.write(repr(key, event))

- ``.flatMap()``
- ``.flatMapValues()``

    .. sourcecode:: python

        @app.agent(topic)
        async def process(stream):
            async for event in stream:
                # split sentences into words
                for word in event.text.split():
                    yield event.derive(text=word)

- ``.branch()``

    This is a special case of `filter` in KS, in Faust just
    write code and forward events as appropriate:

    .. sourcecode:: python

        app = faust.App('transfer-demo')

        # source topic
        source_topic = app.topic('transfers')

        # destination topics
        tiny_transfers = app.topic('tiny_transfers')
        small_transfers = app.topic('small_transfers')
        large_transfers = app.topic('large_transfers')


        @app.agent(source_topic)
        async def process(stream):
            async for event in stream:
                if event.amount >= 1000.0:
                    event.forward(large_transfers)
                elif event.amount >= 100.0:
                    event.forward(small_transfers)
                else:
                    event.forward(tiny_transfers)

- ``.through()``:

    .. sourcecode:: python

        @app.agent(topic)
        async def process(stream):
            async for event in stream.through('other-topic'):
                yield event

- ``.to()``:

    .. sourcecode:: python

        app = faust.App('to-demo')
        source_topic = app.topic('source')
        other_topic = app.topic('other')

        @app.agent(source_topic)
        async def process(stream):
            async for event in stream:
                event.forward(other_topic)

- ``.selectKey()``

    Just transform the key yourself:

    .. sourcecode:: python

        @app.agent(source_topic)
        async def process(stream):
            async for key, value in stream.items():
                key = format_key(key)

    If you want to transform the key for processors to use, then you
    have to change the current context to have the new key:

    .. sourcecode:: python

        @app.agent(source_topic)
        async def process(stream):
            async for event in stream:
                event.req.key = format_key(event.req.key)

- ``groupBy()``

    .. sourcecode:: python

        @app.agent(source_topic)
        async def process(stream):
            async for event in stream.group_by(Withdrawal.account):
                yield event

- ``groupByKey()``

    ???

- ``.transform()``
- ``.transformValues()``

    ???

- ``.process()``

    Process in KS calls a Processor and is usually used to also call periodic
    actions (punctuation).  In Faust you'd rather create a background task:

    .. sourcecode:: python

        import faust

        # Useless example collecting transfer events
        # and summing them up after one second.

        class Transfer(faust.Record, serializer='json'):
            amount: float

        app = faust.App('transfer-demo')
        transfer_topic = app.topic('transfers', value_type=Transfer)

        class TransferBuffer:

            def __init__(self):
                self.pending = []
                self.total = 0

            def flush(self):
                for amount in self.pending:
                    self.total += amount
                self.pending.clear()
                print('TOTAL NOW: %r' % (total,))

            def add(self, amount):
                self.pending.append(amount)
        buffer = TransferBuffer()

        @app.agent(transfer_topic)
        async def task(transfers):
            async for transfer in transfers:
                buffer.add(transfer.amount)

        @app.timer(interval=1.0)
        async def flush_buffer():
            buffer.flush()

        if __name__ == '__main__':
            app.main()

- ``join()``
- ``outerJoin()``
- ``leftJoin()``

    NOT IMPLEMENTED

    .. sourcecode:: python

        async for event in (s1 & s2).join()
        async for event in (s1 & s2).outer_join()
        async for event in (s1 & s2).left_join()
