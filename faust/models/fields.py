import inspect
import sys
from datetime import datetime
from decimal import Decimal, DecimalTuple
from functools import lru_cache
from operator import attrgetter
from typing import (
    Any,
    Iterable,
    Mapping,
    Optional,
    Type,
    TypeVar,
    cast,
)

from mode.utils.text import pluralize

from faust.exceptions import ValidationError
from faust.types.models import (
    FieldDescriptorT,
    ModelT,
    T,
)
from faust.utils import iso8601

__all__ = [
    'TYPE_TO_FIELD',
    'FieldDescriptor',
    'NumberField',
    'FloatField',
    'IntegerField',
    'DatetimeField',
    'DecimalField',
    'BytesField',
    'StringField',
    'field_for_type',
]

CharacterType = TypeVar('CharacterType', str, bytes)


def _is_concrete_model(typ: Type = None) -> bool:
    return (typ is not None and
            inspect.isclass(typ) and
            issubclass(typ, ModelT) and
            typ is not ModelT and
            not getattr(typ, '__is_abstract__', False))


class FieldDescriptor(FieldDescriptorT[T]):
    """Describes a field.

    Used for every field in Record so that they can be used in join's
    /group_by etc.

    Examples:
        >>> class Withdrawal(Record):
        ...    account_id: str
        ...    amount: float = 0.0

        >>> Withdrawal.account_id
        <FieldDescriptor: Withdrawal.account_id: str>
        >>> Withdrawal.amount
        <FieldDescriptor: Withdrawal.amount: float = 0.0>

    Arguments:
        field (str): Name of field.
        type (Type): Field value type.
        required (bool): Set to false if field is optional.
        default (Any): Default value when `required=False`.

    Keyword Arguments:
        model (Type): Model class the field belongs to.
        parent (FieldDescriptorT): parent field if any.
    """

    #: Name of attribute on Model.
    field: str

    #: Type of value (e.g. ``int``, or ``Optional[int]``)).
    type: Type[T]

    #: The model class this field is associated with.
    model: Type[ModelT]

    #: If this holds a generic type such as list/set/dict then
    #: this holds the type of collection.
    generic_type: Optional[Type]
    member_type: Optional[Type]

    #: Set if a value for this field is required (cannot be :const:`None`).
    required: bool = True

    #: Default value for non-required field.
    default: Optional[T] = None  # noqa: E704

    coerce: bool = False

    def __init__(self, *,
                 field: str = None,
                 type: Type[T] = None,
                 model: Type[ModelT] = None,
                 required: bool = True,
                 default: T = None,
                 parent: FieldDescriptorT = None,
                 coerce: bool = None,
                 generic_type: Type = None,
                 member_type: Type = None,
                 **options: Any) -> None:
        self.field = cast(str, field)
        self.type = cast(Type[T], type)
        self.model = cast(Type[ModelT], model)
        self.required = required
        self.default = default
        self.parent = parent
        self.generic_type = generic_type
        self.member_type = member_type
        self._copy_descriptors(self.type)
        if coerce is not None:
            self.coerce = coerce
        self.options = options

    def __set_name__(self, owner: Type[ModelT], name: str) -> None:
        self.model = owner
        self.field = name

    def clone(self, **kwargs: Any) -> FieldDescriptorT:
        return type(self)(**{**self.as_dict(), **kwargs})

    def as_dict(self) -> Mapping[str, Any]:
        return {
            'field': self.field,
            'type': self.type,
            'model': self.model,
            'required': self.required,
            'default': self.default,
            'parent': self.parent,
            'coerce': self.coerce,
            'generic_type': self.generic_type,
            'member_type': self.member_type,
            **self.options,
        }

    def validate(self, value: T) -> Iterable[ValidationError]:
        return []

    def prepare_value(self, value: Any) -> Optional[T]:
        return cast(T, value)

    def _copy_descriptors(self, typ: Type = None) -> None:
        if typ is not None and _is_concrete_model(typ):
            typ._contribute_field_descriptors(self, typ._options, parent=self)

    def __get__(self, instance: Any, owner: Type) -> Any:
        # class attribute accessed
        if instance is None:
            return self

        # instance attribute accessed
        return instance.__dict__[self.field]

    def should_coerce(self, value: Any) -> bool:
        return self.coerce and (self.required or value is not None)

    def getattr(self, obj: ModelT) -> T:
        """Get attribute from model recursively.

        Supports recursive lookups e.g. ``model.getattr('x.y.z')``.
        """
        return attrgetter('.'.join(reversed(list(self._parents_path()))))(obj)

    def _parents_path(self) -> Iterable[str]:
        node: Optional[FieldDescriptorT] = self
        while node:
            yield node.field
            node = node.parent

    def validation_error(self, reason: str) -> ValidationError:
        return ValidationError(reason, field=self)

    def __set__(self, instance: Any, value: T) -> None:
        instance.__dict__[self.field] = value

    def __repr__(self) -> str:
        default = '' if self.required else f' = {self.default!r}'
        typ = getattr(self.type, '__name__', self.type)
        return f'<{type(self).__name__}: {self.ident}: {typ}{default}>'

    @property
    def ident(self) -> str:
        """Return the fields identifier."""
        return f'{self.model.__name__}.{self.field}'


class NumberField(FieldDescriptor[T]):

    max_value: Optional[int]
    min_value: Optional[int]

    def __init__(self, *,
                 max_value: int = None,
                 min_value: int = None,
                 **kwargs: Any) -> None:
        self.max_value = max_value
        self.min_value = min_value

        super().__init__(**kwargs, **{
            'max_value': max_value,
            'min_value': min_value,
        })

    def validate(self, value: T) -> Iterable[ValidationError]:
        val = cast(int, value)
        max_ = self.max_value
        if max_:
            if val > max_:
                yield self.validation_error(
                    f'{self.field} cannot be more than {max_}')
        min_ = self.min_value
        if min_:
            if val < min_:
                yield self.validation_error(
                    f'{self.field} must be at least {min_}')


class IntegerField(NumberField[int]):

    def prepare_value(self, value: Any) -> Optional[int]:
        return int(value) if self.should_coerce(value) else value


class FloatField(NumberField[float]):

    def prepare_value(self, value: Any) -> Optional[float]:
        return float(value) if self.should_coerce(value) else value


class DecimalField(NumberField[Decimal]):
    max_digits: Optional[int] = None
    max_decimal_places: Optional[int] = None

    def __init__(self, *,
                 max_digits: int = None,
                 max_decimal_places: int = None,
                 **kwargs: Any) -> None:
        self.max_digits = max_digits
        self.max_decimal_places = max_decimal_places

        super().__init__(**kwargs, **{
            'max_digits': max_digits,
            'max_decimal_places': max_decimal_places,
        })

    def prepare_value(self, value: Any) -> Optional[Decimal]:
        return Decimal(value) if self.should_coerce(value) else value

    def validate(self, value: Decimal) -> Iterable[ValidationError]:
        if not value.is_finite():  # check for Inf/NaN/sNaN/qNaN
            yield self.validation_error(f'Illegal value in decimal: {value!r}')

        decimal_tuple: Optional[DecimalTuple] = None

        mdp = self.max_decimal_places
        if mdp:
            if decimal_tuple is None:
                decimal_tuple = value.as_tuple()
            if abs(decimal_tuple.exponent) > mdp:
                yield self.validation_error(
                    f'{self.field} must have less than {mdp} decimal places.')
        max_digits = self.max_digits
        if max_digits:
            if decimal_tuple is None:
                decimal_tuple = value.as_tuple()
            digits = len(decimal_tuple.digits[:decimal_tuple.exponent])
            if digits > max_digits:
                yield self.validation_error(
                    f'{self.field} must have less than {max_digits} digits.')


class CharField(FieldDescriptor[CharacterType]):

    max_length: Optional[int]
    min_length: Optional[int]
    trim_whitespace: bool
    allow_blank: bool

    def __init__(self, *,
                 max_length: int = None,
                 min_length: int = None,
                 trim_whitespace: bool = False,
                 allow_blank: bool = False,
                 **kwargs: Any) -> None:
        self.max_length = max_length
        self.min_length = min_length
        self.trim_whitespace = trim_whitespace
        self.allow_blank = allow_blank
        super().__init__(
            **kwargs,
            **{'max_length': max_length,
               'min_length': min_length,
               'trim_whitespace': trim_whitespace,
               'allow_blank': allow_blank},
        )

    def validate(self, value: CharacterType) -> Iterable[ValidationError]:
        allow_blank = self.allow_blank
        if not allow_blank and not len(value):
            yield self.validation_error(f'{self.field} cannot be left blank')
        max_ = self.max_length
        length = len(value)
        min_ = self.min_length
        if min_:
            if length < min_:
                chars = pluralize(min_, 'character')
                yield self.validation_error(
                    f'{self.field} must have at least {min_} {chars}')
        if max_:
            if length > max_:
                chars = pluralize(max_, 'character')
                yield self.validation_error(
                    f'{self.field} must be at least {max_} {chars}')


class StringField(CharField[str]):

    def prepare_value(self, value: Any) -> Optional[str]:
        if self.should_coerce(value):
            val = str(value) if not isinstance(value, str) else value
            if self.trim_whitespace:
                return val.strip()
            return val
        else:
            return value


class DatetimeField(FieldDescriptor[datetime]):
    coerce = True  # always coerces, for info only

    def prepare_value(self, value: Any) -> Optional[datetime]:
        if value is not None and not isinstance(value, datetime):
            return iso8601.parse(value)
        else:
            return value


class BytesField(CharField[bytes]):
    encoding: str = sys.getdefaultencoding()
    errors: str = 'strict'

    def __init__(self, *,
                 encoding: str = None,
                 errors: str = None,
                 **kwargs: Any) -> None:
        if encoding is not None:
            self.encoding = encoding
        if errors is not None:
            self.errors = errors
        super().__init__(
            encoding=self.encoding,
            errors=self.errors,
            **kwargs,
        )

    def prepare_value(self, value: Any) -> Optional[bytes]:
        if self.should_coerce(value):
            if isinstance(value, str):
                val = value.encode(encoding=self.encoding)
            if self.trim_whitespace:
                return val.strip()
            return val
        else:
            return value


TYPE_TO_FIELD = {
    int: IntegerField,
    float: FloatField,
    Decimal: DecimalField,
    str: StringField,
    bytes: BytesField,
    datetime: DatetimeField,
}


@lru_cache(maxsize=2048)
def field_for_type(typ: Type) -> Type[FieldDescriptorT]:
    try:
        return TYPE_TO_FIELD[typ]
    except KeyError:
        for basecls, DescriptorType in TYPE_TO_FIELD.items():
            try:
                if issubclass(typ, basecls):
                    return DescriptorType
            except TypeError:
                # not a type that can be used with issubclass
                # so skip trying
                break
        return FieldDescriptor
