import faust
from .app import app
from . import products


class Order(faust.Record):
    id: str
    price: float
    quantity: int
    user_id: int
    product_id: int

    count_by_user = app.Table(default=int)

    def update_count(self, n: int = 1) -> None:
        self.count_by_user[self.user_id] += n

    @property
    def num(self) -> int:
        return self.count_by_user[self.user_id]


@app.agent(value_type=Order)
async def process_order(orders):
    # repartition the stream by user
    async for order in orders.group_by(Order.user_id):
        order.update_count(1)
        print(f'#{order.num} {order.user_id} ordered {order.product_id}')
        await products.reserve.send(key=order.product_id, value=order.quantity)


@app.command()
async def add_example_orders():
    """Produce example orders (our custom command)."""
    import random
    # let's make some customers
    for i in range(10):
        await process_order.send(
            value=Order(
                id=faust.uuid(),
                price=i,
                quantity=1,
                user_id=random.choice([1, 2, 3]),
                product_id=random.choice([1, 2, 3]),
            ),
        )


if __name__ == '__main__':
    app.main()
