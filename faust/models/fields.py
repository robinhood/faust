import inspect
from decimal import Decimal
from operator import attrgetter
from typing import (
    Any,
    Iterable,
    Mapping,
    Optional,
    Type,
    cast,
)

from mode.utils.text import pluralize

from faust.exceptions import ValidationError
from faust.types.models import (
    FieldDescriptorT,
    ModelT,
    T,
)


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

    #: Set if a value for this field is required (cannot be :const:`None`).
    required: bool = True

    #: Default value for non-required field.
    default: Optional[T] = None  # noqa: E704

    def __init__(self, *,
                 field: str = None,
                 type: Type[T] = None,
                 model: Type[ModelT] = None,
                 required: bool = True,
                 default: T = None,
                 parent: FieldDescriptorT = None,
                 **options: Any) -> None:
        self.field = cast(str, field)
        self.type = cast(Type[T], type)
        self.model = cast(Type[ModelT], model)
        self.required = required
        self.default = default
        self.parent = parent
        self._copy_descriptors(self.type)
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
            **self.options,
        }

    def validate(self, value: T) -> Iterable[ValidationError]:
        return []

    def prepare_value(self, value: T) -> T:
        return value

    def _copy_descriptors(self, typ: Type = None) -> None:
        if typ is not None and _is_concrete_model(typ):
            typ._contribute_field_descriptors(self, typ._options, parent=self)

    def __get__(self, instance: Any, owner: Type) -> Any:
        # class attribute accessed
        if instance is None:
            return self

        # instance attribute accessed
        return instance.__dict__[self.field]

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
        typ = self.type.__name__
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


class IntField(NumberField[int]):
    ...


class DecimalField(NumberField[Decimal]):
    max_digits: Optional[Decimal] = None

    def __init__(self, *,
                 max_digits: int = None,
                 max_decimal_places: int = None,
                 max_whole_digits: int = None,
                 **kwargs: Any) -> None:
        if max_digits is not None:
            self.max_digits = Decimal(max_digits)
        self.max_decimal_places = max_decimal_places
        self.max_whole_digits = max_whole_digits

        super().__init__(**kwargs, **{
            'max_digits': max_digits,
            'max_decimal_places': max_decimal_places,
            'max_whole_digits': max_whole_digits,
        })

    def validate(self, value: Decimal) -> Iterable[ValidationError]:
        yield from super().validate(value)

        mdp = self.max_decimal_places
        if mdp:
            _, _, exponent = value.as_tuple()
            if abs(exponent) > mdp:
                yield self.validation_error(
                    f'{self.field} must have less than {mdp} decimal places.')
        max_digits = self.max_digits
        if max_digits:
            if int(value) > max_digits:
                yield self.validation_error(
                    f'{self.field} must have less than {max_digits} digits.')


class StringField(FieldDescriptor[str]):

    max_length: Optional[int]
    min_length: Optional[int]
    trim_whitespace: bool
    allow_blank: bool

    def __init__(self,
                 *args: Any,
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

    def prepare_value(self, value: str) -> str:
        if self.trim_whitespace:
            return value.strip()
        return value

    def validate(self, value: str) -> Iterable[ValidationError]:
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
