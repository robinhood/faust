import faust


def create_app() -> faust.App:
    app = faust.App(
        't.live',
        cache='redis://localhost:6379',
        origin='t.live',
        autodiscover=True,
    )
    app.web.blueprints.add('/order/', 't.live.orders.views:blueprint')
    return app


def create_livecheck(app: faust.App):
    return app.LiveCheck()


app = create_app()
livecheck = create_livecheck(app)
