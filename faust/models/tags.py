import abc
from decimal import Decimal
from typing import Generic, Optional, Type, TypeVar
from faust.exceptions import SecurityError

T = TypeVar('T')


class _PIIValue:
    ...


class Tag(Generic[T]):
    value: T
    field: Optional[str]

    is_secret: bool = False
    is_sensitive: bool = False
    is_personal: bool = False

    def __init__(self, value: T, *,
                 field: str = None):
        if isinstance(value, Tag):
            raise SecurityError('Cannot wrap: value is already tagged')
        self._value = value
        self.field = field

    def __set_name__(self, owner: Type, name: str) -> None:
        self.field = name

    def get_value(self) -> T:
        return self._value

    def __repr__(self) -> str:
        return f'<{self._name}: {self.field}@{id(self):#x}>'

    @abc.abstractmethod
    def __str__(self) -> str:
        ...

    @abc.abstractmethod
    def __format__(self, format_spec: str) -> str:
        ...

    @property
    def _name(self) -> str:
        return type(self).__name__


class OpaqueTag(Tag[T]):

    def __str__(self) -> str:
        raise SecurityError(f'Attempt to use {self._name} data as a string')

    def __format__(self, format_spec: str) -> str:
        raise SecurityError(f'Attempt to use {self._name} data as a string')


class TransparentTag(Tag[T]):

    def __str__(self) -> str:
        return str(self._value)

    def __format__(self, format_spec: str) -> str:
        return self._value.__format__(format_spec)


class Personal(OpaqueTag[T]):
    is_personal = True


class Secret(OpaqueTag[T]):
    is_secret = True

    mask: str = '***********'

    def __str__(self) -> str:
        return self.mask

    def __format__(self, format_spec: str) -> str:
        return self.mask.__format__(format_spec)


class Sensitive(OpaqueTag[T]):
    is_sensitive = True


class _PIIstr(str):
    ...


class _PIIbytes(bytes):
    ...


class _PIIint(int):
    ...


class _PIIfloat(float):
    ...


class _PIIDecimal(Decimal):
    ...
