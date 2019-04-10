from ..app import app
from .models import Order

orders_topic = app.topic('orders', value_type=Order)
execution_topic = app.topic('order-execution', value_type=Order)
