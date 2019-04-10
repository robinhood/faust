import faust


class Order(faust.Record):
    id: str
    user_id: str
    side: str
    quantity: float
    price: float

    # fake orders are not executed.
    fake: bool = False
