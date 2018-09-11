===================================
 Cheatsheet
===================================

.. topic:: Process events in a Kafka topic

    .. sourcecode:: python

        orders_topic = app.topic('orders', value_serializer='json')

        @app.agent(orders_topic)
        async def process_order(orders):
            async for order in orders:
                print(order['product_id'])

.. topic:: Describe stream data using models

    .. sourcecode:: python

        from datetime import datetime
        import faust

        class Order(faust.Record, serializer='json', isodates=True):
            id: str
            user_id: str
            product_id: str
            amount: float
            price: float
            date_created: datatime = None
            date_updated: datetime = None

        orders_topic = app.topic('orders', value_type=Order)

        @app.agent(orders_topic)
        async def process_order(orders):
            async for order in orders:
                print(order.product_id)


.. topic:: Use async. I/O to perform other actions while processing the stream

    .. sourcecode:: python

        # [...]
        @app.agent(orders_topic)
        async def process_order(orders):
            session = aiohttp.ClientSession()
            async for order in orders:
                async with session.get(f'http://e.com/api/{order.id}/') as resp:
                    product_info = await request.text()
                    await session.post(
                        f'http://cache/{order.id}/', data=product_info)

.. topic:: Buffer up many events at a time

    Here we get up to 100 events within a 30 second window:

    .. sourcecode:: python

        # [...]
        async for orders_batch in orders.take(100, within=30.0):
            print(len(orders))

.. topic:: Aggregate information into a table

    .. sourcecode:: python

        orders_by_country = app.Table('orders_by_country', default=int)

        @app.agent(orders_topic)
        async def process_order(orders):
            async for order in orders.group_by(order.country_origin):
                country = order.country_origin
                orders_by_country[country] += 1
                print(f'Orders for country {country}: {orders_by_country[country]}')

.. topic:: Aggregate information using a window

    Count number of orders by country, within the last two days:

    .. sourcecode:: python

        orders_by_country = app.Table(
            'orders_by_country',
            default=int,
        ).hopping(timedelta(days=2))

        async for order in orders_topic.stream():
            orders_by_country[order.country_origin] += 1
            # values in this table are not concrete! access .current
            # for the value related to the time of the current event
            print(orders_by_country[order.country_origin].current())
