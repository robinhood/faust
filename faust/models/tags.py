from typing import Generic, Optional, Type, TypeVar
from faust.exceptions import SecurityError

T = TypeVar('T')


class Tag(Generic[T]):
    is_secret: bool = False
    is_sensitive: bool = False
    is_personal: bool = False


class OpaqueTag(Tag[T]):
    value: T
    field: Optional[str]

    def __init__(self, value: T, *,
                 field: str = None):
        if isinstance(value, Tag):
            raise SecurityError('Cannot wrap: value is already tagged')
        self._value = value
        self.field = field

    def __set_name__(self, owner: Type, name: str):
        self.field = str

    def get_value(self) -> T:
        return self._value

    def __str__(self) -> str:
        raise SecurityError(f'Attempt to use {self._name} data as a string')

    def __format__(self, format_spec: str) -> str:
        raise SecurityError(f'Attempt to use {self._name} data as a string')

    def __repr__(self) -> str:
        return f'<{self._name}: {self.field}@{id(self):#x}>'

    @property
    def _name(self):
        return type(self).__name__


class TransparentTag(Tag[T]):

    def __str__(self) -> str:
        return str(self.get_value())


class Personal(OpaqueTag[T]):
    is_personal = True


class Secret(OpaqueTag[T]):
    is_secret = True

    mask: str = '***********'

    def __str__(self):
        return self.mask

    def __format__(self, format_spec: str):
        return self.mask.__format__(format_spec)


class Sensitive(OpaqueTag[T]):
    is_sensitive = True
