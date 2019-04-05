import faust


def create_app():
    return faust.App(
        'proj324',
        origin='proj324',
        autodiscover=True,
    )


app = create_app()
