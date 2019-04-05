from .faust import app

__all__ = ['app', 'main']


def main() -> None:
    app.main()
