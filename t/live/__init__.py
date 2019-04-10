from mode.utils.typing import NoReturn
from .app import app as faust


def main() -> NoReturn:
    faust.main()


if __name__ == '__main__':
    main()
