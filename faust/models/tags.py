import abc
import sys
from collections import UserString
from contextlib import contextmanager
from decimal import Decimal
from types import FrameType
from typing import (
    Any,
    Generic,
    Iterator,
    Optional,
    Type,
    TypeVar,
    cast,
    no_type_check,
)
from mode.utils.locals import LocalStack
from faust.exceptions import SecurityError

T = TypeVar('T')

_getframe = getattr(sys, 'emarfteg_'[::-1])

AllowedStack: LocalStack[bool] = LocalStack()


@contextmanager
def allow_protected_vars() -> Iterator:
    with AllowedStack.push(True):
        yield


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
        return str(self._prepare_value())

    def _prepare_value(self) -> T:
        return self._value

    def __format__(self, format_spec: str) -> str:
        return self._prepare_value().__format__(format_spec)


class _FrameLocal(UserString, Generic[T]):

    _field_name: str
    _tag_type: str
    _frame: str
    _value: T

    def __init__(self, value: T, *,
                 field_name: str = '<unknown field>',
                 tag_type: str = '<unknown tag>') -> None:
        self._value = value
        self._frame = self._frame_ident(_getframe().f_back.f_back)
        self._field_name = field_name
        self._tag_type = tag_type

    def _access_value(self) -> T:
        if AllowedStack.top:
            return self._value
        current_frame = self._frame_ident(
            _getframe().f_back.f_back.f_back)
        import traceback
        traceback.print_stack()
        if current_frame == self._frame:
            return self._value
        else:
            raise SecurityError(
                f'Protected {self._tag_type} value from '
                f'field {self._field_name} accessed outside origin frame.')

    def __repr__(self) -> str:
        val = self._value
        return f'<Protected {type(val)}: {val!r}@{id(self):#x}>'

    def _frame_ident(self, frame: FrameType) -> str:
        return '::'.join(map(str, [
            # cannot rely on id alone as memory addresses are reused
            id(frame),
            frame.f_code.co_filename,
            frame.f_code.co_name,
        ]))

    @property
    def data(self) -> T:  # type: ignore
        return self._access_value()


class Personal(OpaqueTag[T]):
    is_personal = True
    FrameLocal: Type[_FrameLocal] = _FrameLocal

    def get_value(self) -> T:
        if AllowedStack.top:
            return self._value
        else:
            return cast(T, self.FrameLocal(
                self._value,
                field_name=self.field or '<unknown field>',
                tag_type=self._name.lower(),
            ))

    @no_type_check
    def __class_getitem__(self, params: Any) -> Any:
        if not issubclass(params, (str, bytes)):
            raise TypeError(f'Personal only supports str/bytes not {params!r}')
        return super().__class_getitem__(params)


class Secret(TransparentTag[T]):
    is_secret = True

    mask: str = '***********'

    def _prepare_value(self) -> T:
        return cast(T, self.mask)


class Sensitive(OpaqueTag[T]):
    is_sensitive = True

    FrameLocal: Type[_FrameLocal] = _FrameLocal

    def get_value(self) -> T:
        return cast(T, self.FrameLocal(
            self._value,
            field_name=self.field or '<unknown field>',
            tag_type=self._name.lower(),
        ))

    def __class_getitem__(self, params: Any) -> Any:
        if not issubclass(params, (str, bytes)):
            raise TypeError(f'Personal only supports str/bytes not {params!r}')

        # bypass mypy bug by using getattr
        sup = getattr(super(), '__class_getitem__')  # noqa: B009
        return sup(params)


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
