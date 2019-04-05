import faust


def create_app():
    return faust.App(
        'proj',
        origin='proj',
        autodiscover=True,
    )


app = create_app()
