from uuid import uuid4

__all__ = ['uuid']


def uuid() -> str:
    return str(uuid4())
