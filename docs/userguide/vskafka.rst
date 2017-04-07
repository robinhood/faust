.. _kafka-streams-comparison:

========================
 Faust vs Kafka Streams
========================

KStream
=======

- ``.filter()``
- ``.filterNot()``

    Just use the `if` statement:

    .. code-block:: python

        async for event in stream:
            if event.amount >= 300.0:
                yield event

- ``.map()``

    Just call the function you want:

    .. code-block:: python

        async for key, event in stream.items():
            yield myfun(key, event)

- ``.forEach()``

    In KS ``forEach`` is the same as ``map``, but ends the processing chain.

- ``.peek()``

    In KS ``peek`` is the same as ``map``, but documents that the
    action may have a side effect.

- ``.mapValues()``:

    .. code-block:: python

        async for event in stream.items():
            yield myfun(event)

- ``.print()``:

    .. code-block:: python

        async for event in stream:
            print(event)

- ``.writeAsText()``:

    .. code-block:: python

        async for key, event in stream.items():
            with open(path, 'a') as f:
                f.write(repr(key, event))

- ``.flatMap()``
- ``.flatMapValues()``

    .. code-block:: python

        async for event in stream.items():
            # split sentences into words
            for word in event.text.split():
                yield event.derive(text=word)

- ``.branch()``

    This is a special case of `filter` in KS, in Faust just
    write code and forward events as appropriate:

    .. code-block:: python

        tiny_transfers = faust.topic('tiny_transfers')
        small_transfers = faust.topic('small_transfers')
        large_transfers = faust.topic('large_transfers')

        async for event in stream:
            if event.amount >= 1000.0:
                event.forward(large_transfers)
            elif event.amount >= 100.0:
                event.forward(small_transfers)
            else:
                event.forward(tiny_transfers)

- ``.through()``:

    .. code-block:: python

        async for event in stream.through('topic'):
            yield event

- ``.to()``:

    .. code-block:: python

        other_topic = faust.topic(other)
        async for event in stream:
            event.forward(other_topic)

- ``.selectKey()``

    Just transform the key yourself:

    .. code-block:: python

        async for key, value in stream.items():
            key = format_key(key)

    If you want to transform the key for processors to use, then you
    have to change the current context to have the new key:

    .. code-block:: python

        async for event in stream:
            event.req.key = format_key(event.req.key)

- ``groupBy()``

    NOT IMPLEMENTED

    .. code-block:: python

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

    .. code-block:: python

        import asyncio
        import faust

        # Useless example collecting transfer events
        # and summing them up after one second.

        class Transfer(faust.Record, serializer='json'):
            amount: float

        transfer_topic = faust.topic('transfers', value_type=Transfer)

        class TransferBuffer:

            def __init__(self):
                self.pending = []
                self.total = 0

            async def _flush_events():
                while 1:
                    # flush events every second
                    await asyncio.sleep(1.0)
                    for amount in self.pending:
                        self.total += total
                    self.pending.clear()
                    print('TOTAL NOW: %r' % (total,))

            def add(self, amount):
                self.pending.append(amount)

        app = faust.App('transfer-demo')

        async def task(app);
            buffer = TransferBuffer()
            async for transfer in app.stream(transfer_topic):
                buffer.add(transfer.amount)

        async def main():
            app.add_task(task())

        if __name__ == '__main__':
            faust.Worker(app).execute_from_commandline(main())

- ``join()``
- ``outerJoin()``
- ``leftJoin()``

    NOT IMPLEMENTED

    .. code-block:: python

        async for event in (s1 & s2).join()
        async for event in (s1 & s2).outer_join()
        async for event in (s1 & s2).left_join()
