.. _guide-testing:

=================================================
 Testing
=================================================

.. contents::
    :local:
    :depth: 2

.. module:: faust

.. currentmodule:: faust

Basics
======

To test an agent when unit testing or functional testing, use the special
``Agent.test()`` mode to send items to the stream while processing it locally:

.. sourcecode:: python

    app = faust.App('test-example')

    class Order(faust.Record, serializer='json'):
        account_id: str
        product_id: str
        amount: int
        price: float

    orders_topic = app.topic('orders', value_type=Order)
    orders_for_account = app.Table('order-count-by-account', default=int)

    @app.agent(orders_topic)
    async def order(orders):
        async for order in orders.group_by(Order.account_id):
            orders_for_account[order.account_id] += 1
            yield order

Our agent reads a stream of orders and keeps a count of them by account id
in a distributed table also partitioned by the account id.

To test this agent we use ``order.test_context()``:

.. sourcecode:: python

    async def test_order():
        # start and stop the agent in this block
        async with order.test_context() as agent:
            order = Order(account_id='1', product_id='2', amount=1, price=300)
            # sent order to the test agents local channel, and wait
            # the agent to process it.
            await agent.put(order)
            # at this point the agent already updated the table
            assert orders_for_account[order.account_id] == 1
            await agent.put(order)
            assert orders_for_account[order.account_id] == 2

    async def run_tests():
        app.conf.store = 'memory://'   # tables must be in-memory
        await test_order()

    if __name__ == '__main__':
        import asyncio
        loop = asyncio.get_event_loop()
        loop.run_until_complete(run_tests())


For the rest of this guide we'll be using :pypi:`pytest` and
:pypi:`pytest-asyncio` for our examples. If you're using a different
testing framework you may have to adapt them a bit to work.

Testing with :pypi:`pytest`
===========================

Testing that an agent sends to topic/calls another agent.
---------------------------------------------------------

When unit testing you should mock any dependencies of the agent being tested,

- If your agent calls another function: mock that function to verify it was
  called.

- If your agent sends a message to a topic: mock that topic to verify
  a message was sent.

- If your agent calls another agent: mock the other agent to verify it
  was called.

Here's an example agent that calls another agent:

.. sourcecode:: python

    import faust

    app = faust.App('example-test-agent-call')

    @app.agent()
    async def foo(stream):
        async for value in stream:
            await bar.send(value)
            yield value

    @app.agent()
    async def bar(stream):
        async for value in stream:
            yield value + 'YOLO'

To test these two agents you have to test them in isolation of each other:
first test ``foo`` with ``bar`` mocked, then in a different test do ``bar``:

.. sourcecode:: python

    import pytest
    from unittest.mock import Mock, patch

    from example import app, foo, bar

    @pytest.fixture()
    def test_app(event_loop):
        """passing in event_loop helps avoid 'attached to a different loop' error"""
        app.finalize()
        app.conf.store = 'memory://'
        app.flow_control.resume()
        return app

    @pytest.mark.asyncio()
    async def test_foo(test_app):
        with patch(__name__ + '.bar') as mocked_bar:
            mocked_bar.send = mock_coro()
            async with foo.test_context() as agent:
                await agent.put('hey')
                mocked_bar.send.assert_called_with('hey')

    def mock_coro(return_value=None, **kwargs):
        """Create mock coroutine function."""
        async def wrapped(*args, **kwargs):
            return return_value
        return Mock(wraps=wrapped, **kwargs)

    @pytest.mark.asyncio()
    async def test_bar(test_app):
        async with bar.test_context() as agent:
            event = await agent.put('hey')
            assert agent.results[event.message.offset] == 'heyYOLO'

.. note::

    The :pypi:`pytest-asyncio` extension must be installed to run these tests.
    If you don't have it use :program:`pip` to install it:

    .. sourcecode:: console

        $ pip install -U pytest-asyncio


Testing and windowed tables
===========================

If your table is windowed and you want to verify that the value for a key is
correctly set, use ``table[k].current(event)`` to get the value placed within
the window of the current event:

.. sourcecode:: python

    import faust
    import pytest

    @pytest.mark.asyncio()
    async def test_process_order():
        app.conf.store = 'memory://'
        async with order.test_context() as agent:
            order = Order(account_id='1', product_id='2', amount=1, price=300)
            event = await agent.put(order)

            # windowed table: we select window relative to the current event
            assert orders_for_account['1'].current(event) == 1

            # in the window 3 hours ago there were no orders:
            assert orders_for_account['1'].delta(3600 * 3, event)


    class Order(faust.Record, serializer='json'):
        account_id: str
        product_id: str
        amount: int
        price: float

    app = faust.App('test-example')
    orders_topic = app.topic('orders', value_type=Order)

    # order count within the last hour (window is a 1-hour TumblingWindow).
    orders_for_account = app.Table(
        'order-count-by-account', default=int,
    ).tumbling(3600).relative_to_stream()

    @app.agent(orders_topic)
    async def process_order(orders):
        async for order in orders.group_by(Order.account_id):
            orders_for_account[order.account_id] += 1
            yield order
