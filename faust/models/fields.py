import inspect
import sys
from datetime import datetime
from decimal import Decimal, DecimalTuple
from functools import lru_cache
from operator import attrgetter
from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    cast,
)

from mode.utils.objects import cached_property
from mode.utils.text import pluralize

from faust.exceptions import ValidationError
from faust.types.models import (
    FieldDescriptorT,
    ModelT,
    T,
)
from faust.utils import iso8601

from .tags import Tag
from .typing import NodeType, TypeExpression

__all__ = [
    'TYPE_TO_FIELD',
    'FieldDescriptor',
    'BooleanField',
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

    #: Name of field in serialized data (if differs from :attr:`field`).
    #: Defaults to :attr:`field`.
    input_name: str

    #: Name of field when serializing data (if differs from :attr:`field`).
    #: Defaults to :attr:`input_name`.
    output_name: str

    #: Type of value (e.g. ``int``, or ``Optional[int]``)).
    type: Type[T]

    #: The model class this field is associated with.
    model: Type[ModelT]

    #: Set if a value for this field is required (cannot be :const:`None`).
    required: bool = True

    #: Default value for non-required field.
    default: Optional[T] = None  # noqa: E704

    #: Coerce value to field descriptors type.
    #: This means assigning a value to this field, will first convert
    #: the value to the requested type.
    #: For example for a :class:`FloatField` the input will be converted
    #: to float, and passing any value that cannot be converted to float
    #: will raise an error.
    #:
    #: If coerce is not enabled you can store any type of value.
    #:
    #: Note: :const:`None` is usually considered a valid value for any field
    #: but this depends on the descriptor type.
    coerce: bool = False

    #: Exclude field from model representation.
    #: This means the field will not be part of the serialized structure.
    #: (``Model.dumps()``, ``Model.asdict()``, and
    #: ``Model.to_representation()``).
    exclude: bool = False

    #: Field may be tagged with Secret/Sensitive/etc.
    tag: Optional[Type[Tag]]

    _to_python: Optional[Callable[[T], T]]
    _expr: Optional[TypeExpression]

    def __init__(self, *,
                 field: str = None,
                 input_name: str = None,
                 output_name: str = None,
                 type: Type[T] = None,
                 model: Type[ModelT] = None,
                 required: bool = True,
                 default: T = None,
                 parent: FieldDescriptorT = None,
                 coerce: bool = None,
                 exclude: bool = None,
                 date_parser: Callable[[Any], datetime] = None,
                 tag: Type[Tag] = None,
                 **options: Any) -> None:
        self.field = cast(str, field)
        self.input_name = cast(str, input_name or field)
        self.output_name = output_name or self.input_name
        self.type = cast(Type[T], type)
        self.model = cast(Type[ModelT], model)
        self.required = required
        self.default = default
        self.parent = parent
        self._copy_descriptors(self.type)
        if coerce is not None:
            self.coerce = coerce
        if exclude is not None:
            self.exclude = exclude
        self.options = options
        if date_parser is None:
            date_parser = iso8601.parse
        self.date_parser: Callable[[Any], datetime] = date_parser
        self.tag = tag
        self._to_python = None
        self._expr = None

    def on_model_attached(self) -> None:
        self._expr = self._prepare_type_expression()
        self._to_python = self._compile_type_expression()

    def _prepare_type_expression(self) -> TypeExpression:
        assert self.model is not None
        expr = TypeExpression(
            self.type,
            user_types=self.model._options.coercions,
            date_parser=self.date_parser,
        )
        return expr

    def _compile_type_expression(self) -> Optional[Callable[[T], T]]:
        assert self._expr is not None
        expr = self._expr
        comprehension = expr.as_function(stacklevel=2)
        if (expr.has_generic_types and
                expr.has_custom_types) or expr.has_nonfield_types:
            return cast(Callable, comprehension)
        return None

    def __set_name__(self, owner: Type[ModelT], name: str) -> None:
        self.model = owner
        self.field = name

    def clone(self, **kwargs: Any) -> FieldDescriptorT:
        return type(self)(**{**self.as_dict(), **kwargs})

    def as_dict(self) -> Mapping[str, Any]:
        return {
            'field': self.field,
            'input_name': self.input_name,
            'output_name': self.output_name,
            'type': self.type,
            'model': self.model,
            'required': self.required,
            'default': self.default,
            'parent': self.parent,
            'coerce': self.coerce,
            'exclude': self.exclude,
            'date_parser': self.date_parser,
            'tag': self.tag,
            **self.options,
        }

    def validate_all(self, value: Any) -> Iterable[ValidationError]:
        need_coercion = not self.coerce
        try:
            v = self.prepare_value(value, coerce=need_coercion)
        except (TypeError, ValueError) as exc:
            vt = type(value)
            yield self.validation_error(
                f'{self.field} is not correct type for {self}, '
                f'got {vt!r}: {exc!r}')
        except Exception as exc:
            yield self.validation_error(
                f'{self.field} got internal error for value {value!r} '
                f'{exc!r}')
        else:
            if v is not None or self.required:
                yield from self.validate(cast(T, v))

    def validate(self, value: T) -> Iterable[ValidationError]:
        return iter([])

    def to_python(self, value: Any) -> Optional[T]:
        to_python = self._to_python
        if to_python is not None:
            value = to_python(value)
        return self.prepare_value(value)

    def prepare_value(self, value: Any, *,
                      coerce: bool = None) -> Optional[T]:
        return cast(T, value)

    def _copy_descriptors(self, typ: Type = None) -> None:
        if typ is not None and _is_concrete_model(typ):
            typ._contribute_field_descriptors(self, typ._options, parent=self)

    def __get__(self, instance: Any, owner: Type) -> Any:
        # class attribute accessed
        if instance is None:
            return self

        field = self.field
        instance_dict = instance.__dict__
        to_python = self._to_python
        value = instance_dict[field]
        if self.lazy_coercion and to_python is not None:
            evaluated_fields: Set[str]
            evaluated_fields = instance.__evaluated_fields__
            if field not in evaluated_fields:
                if value is not None or self.required:
                    value = instance_dict[field] = to_python(value)
                evaluated_fields.add(field)
        return value

    def should_coerce(self, value: Any, coerce: bool = None) -> bool:
        c = coerce if coerce is not None else self.coerce
        return c and (self.required or value is not None)

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
        value = cast(T, self.prepare_value(value))
        if self.tag:
            value = cast(T, self.tag(value, field=self.field))
        else:
            value = value
        instance.__dict__[self.field] = value

    def __repr__(self) -> str:
        default = '' if self.required else f' = {self.default!r}'
        typ = getattr(self.type, '__name__', self.type)
        return f'<{type(self).__name__}: {self.ident}: {typ}{default}>'

    @property
    def ident(self) -> str:
        """Return the fields identifier."""
        return f'{self.model.__name__}.{self.field}'

    @cached_property
    def related_models(self) -> Set[Type[ModelT]]:
        assert self._expr is not None
        return self._expr.found_types[NodeType.MODEL]

    @cached_property
    def lazy_coercion(self) -> bool:
        assert self._expr is not None
        return self._expr.has_generic_types or self._expr.has_models


class BooleanField(FieldDescriptor[bool]):

    def validate(self, value: T) -> Iterable[ValidationError]:
        if not isinstance(value, bool):
            yield self.validation_error(
                f'{self.field} must be True or False, of type bool')

    def prepare_value(self, value: Any, *,
                      coerce: bool = None) -> Optional[bool]:
        if self.should_coerce(value, coerce):
            return True if value else False
        return value


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

    def prepare_value(self, value: Any, *,
                      coerce: bool = None) -> Optional[int]:
        return int(value) if self.should_coerce(value, coerce) else value


class FloatField(NumberField[float]):

    def prepare_value(self, value: Any, *,
                      coerce: bool = None) -> Optional[float]:
        return float(value) if self.should_coerce(value, coerce) else value


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

    def to_python(self, value: Any) -> Any:
        if self._to_python is None:
            if self.model._options.decimals:
                return self.prepare_value(value, coerce=True)
            return self.prepare_value(value)
        else:
            return self._to_python(value)

    def prepare_value(self, value: Any, *,
                      coerce: bool = None) -> Optional[Decimal]:
        return Decimal(value) if self.should_coerce(value, coerce) else value

    def validate(self, value: Decimal) -> Iterable[ValidationError]:
        if not value.is_finite():  # check for Inf/NaN/sNaN/qNaN
            yield self.validation_error(f'Illegal value in decimal: {value!r}')

        decimal_tuple: Optional[DecimalTuple] = None

        mdp = self.max_decimal_places
        if mdp:
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

    def prepare_value(self, value: Any, *,
                      coerce: bool = None) -> Optional[str]:
        if self.should_coerce(value, coerce):
            val = str(value) if not isinstance(value, str) else value
            if self.trim_whitespace:
                return val.strip()
            return val
        else:
            return value


class DatetimeField(FieldDescriptor[datetime]):

    def to_python(self, value: Any) -> Any:
        if self._to_python is None:
            if self.model._options.isodates:
                return self.prepare_value(value, coerce=True)
            return self.prepare_value(value)
        else:
            return self._to_python(value)

    def prepare_value(self, value: Any, *,
                      coerce: bool = None) -> Optional[datetime]:
        if self.should_coerce(value, coerce):
            if value is not None and not isinstance(value, datetime):
                return self.date_parser(value)
            else:
                return value
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

    def prepare_value(self, value: Any, *,
                      coerce: bool = None) -> Optional[bytes]:
        if self.should_coerce(value, coerce):
            if isinstance(value, bytes):
                val = value
            else:
                val = cast(str, value).encode(encoding=self.encoding)
            if self.trim_whitespace:
                return val.strip()
            return val
        else:
            return value


TYPE_TO_FIELD = {
    bool: BooleanField,
    int: IntegerField,
    float: FloatField,
    Decimal: DecimalField,
    str: StringField,
    bytes: BytesField,
    datetime: DatetimeField,
}


@lru_cache(maxsize=2048)
def field_for_type(
        typ: Type) -> Tuple[Type[FieldDescriptorT], Optional[Type[Tag]]]:
    try:
        # 1) Check if type is in fast index.
        return TYPE_TO_FIELD[typ], None
    except KeyError:
        # 2) Unwrap Tag fields to their destination type.
        try:
            origin = typ.__origin__
        except AttributeError:
            pass
        else:
            try:
                if origin is not None and issubclass(origin, Tag):
                    return field_for_type(typ.__args__[0])[0], typ
            except TypeError:
                pass

        # 3) Check if type is a subclass of any of the types
        # in TYPE_TO_FIELD
        for basecls, DescriptorType in TYPE_TO_FIELD.items():
            try:
                if issubclass(typ, basecls):
                    return DescriptorType, None
            except TypeError:
                # not a type that can be used with issubclass
                # so skip trying
                break
        return FieldDescriptor, None
