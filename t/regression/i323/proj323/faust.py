import faust


def create_app():
    return faust.App(
        'proj323',
        origin='proj323',
        autodiscover=True,
    )


app = create_app()
