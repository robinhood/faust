import faust

app = faust.App(
    'proj',
    origin='proj',
    autodiscover=True,
)
