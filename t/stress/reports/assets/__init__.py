from pathlib import Path


def get_path():
    return Path(__file__).absolute().parent
