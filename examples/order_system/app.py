import faust

app = faust.App(
    'orders',   # group/app unique identifier
    version=2,
    origin='order_system',
    autodiscover=True,

    # Faust requires Kafka
    # and we have that running on our machine
    broker='kafka://localhost:9092',
)


if __name__ == '__main__':
    app.main()
