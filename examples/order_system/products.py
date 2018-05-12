import faust
from .app import app


class Product(faust.Record):
    id: int
    name: str

    objects = app.Table()
    quantities = app.Table(default=int)
    quantities_last_hour = app.Table(default=int).hopping(3600, step=60)

    def increase_quantity(self, n: int = 1) -> int:
        self.quantities_last_hour[self.id] += n
        self.quantities[self.id] += n
        return self.quantities[self.id]

    def decrease_quantity(self, n: int = 1) -> int:
        self.quantities_last_hour[self.id] += n
        self.quantities[self.id] -= n
        return self.quantities[self.id]


@app.agent(value_type=Product)
async def register(products):
    async for product in products.group_by(Product.id):
        print(f'Registering product: {product}')
        Product.objects.setdefault(product.id, product)


@app.agent(value_type=int)
async def refill(products):
    async for product_id, amount in products.items():
        product = Product.objects[product_id]
        q_now = product.increase_quantity(amount)
        print(f'Refilled {product} by {amount} to {q_now}')


@app.agent(key_type=int, value_type=int)
async def reserve(product_ids):
    async for product_id, quantity in product_ids.items():
        product = Product.objects[product_id]
        remaining = product.decrease_quantity(quantity)
        print(f'Quantity of {product_id} now: {remaining}')


@app.command()
async def add_example_products():
    product1 = Product(1, 'Nintendo Switch')
    await register.send(value=product1)
    await refill.send(product1.id, 100)

    product2 = Product(2, 'XBOX')
    await register.send(value=product2)
    await refill.send(product2.id, 30)

    product3 = Product(3, 'Playstation')
    await register.send(value=product3)
    await refill.send(product3.id, 60)
