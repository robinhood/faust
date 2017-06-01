from ..types import AppT, AsyncSerializerT

__all__ = ['AsyncSerializer']


class AsyncSerializer(AsyncSerializerT):

    def __init__(self, app: AppT) -> None:
        self.app = app
