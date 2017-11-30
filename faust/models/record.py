"""Record - Dictionary Model."""
from datetime import datetime
from typing import (
    Any, Callable, ClassVar, Dict, Iterable,
    Mapping, Optional, Sequence, Tuple, Type, cast,
)
from .base import FieldDescriptor, Model
from ..serializers.avro import to_avro_type
from ..types.models import Converter, FieldDescriptorT, ModelOptions, ModelT
from ..utils import iso8601
from ..utils.objects import annotations, guess_concrete_type
from ..utils.text import pluralize

__all__ = ['Record']

DATE_TYPES = (datetime,)


def _is_model(cls: Type) -> bool:
    # This is used only to see if cls is a Model type.
    try:
        # Check for List[Model], Set[Model], etc.
        _, cls = guess_concrete_type(cls)
    except TypeError:
        pass
    try:
        return issubclass(cls, ModelT)
    except TypeError:  # typing.Any cannot be used with subclass
        return False


def _is_date(cls: Type,
             *,
             types: Tuple[Type, ...] = DATE_TYPES) -> bool:
    try:
        # Check for List[int], Mapping[int, int], etc.
        _, cls = guess_concrete_type(cls)
    except TypeError:
        pass
    try:
        return issubclass(cls, types)
    except TypeError:
        return False


class Record(Model):
    """Describes a model type that is a record (Mapping).

    Examples:
        >>> class LogEvent(Record, serializer='json'):
        ...     severity: str
        ...     message: str
        ...     timestamp: float
        ...     optional_field: str = 'default value'

        >>> event = LogEvent(
        ...     severity='error',
        ...     message='Broken pact',
        ...     timestamp=666.0,
        ... )

        >>> event.severity
        'error'

        >>> serialized = event.dumps()
        '{"severity": "error", "message": "Broken pact", "timestamp": 666.0}'

        >>> restored = LogEvent.loads(serialized)
        <LogEvent: severity='error', message='Broken pact', timestamp=666.0>

        >>> # You can also subclass a Record to create a new record
        >>> # with additional fields
        >>> class RemoteLogEvent(LogEvent):
        ...     url: str

        >>> # You can also refer to record fields and pass them around:
        >>> LogEvent.severity
        >>> <FieldDescriptor: LogEvent.severity (str)>
    """

    _schema_type: ClassVar[str] = 'record'

    @classmethod
    def _schema_fields(cls) -> Sequence[Mapping]:
        return [
            {'name': key, 'type': to_avro_type(typ)}
            for key, typ in cls._options.fields.items()
        ]

    @classmethod
    def _contribute_to_options(cls, options: ModelOptions) -> None:
        # Find attributes and their types, and create indexes for these.
        # This only happens once when the class is created, so Faust
        # models are fast at runtime.
        fields, defaults = annotations(cls, stop=Record)
        options.fields = cast(Mapping, fields)
        options.fieldset = frozenset(fields)
        options.fieldpos = {i: k for i, k in enumerate(fields.keys())}
        options.optionalset = frozenset(defaults)
        is_model = _is_model
        is_date = _is_date

        # extract all default values, but only for actual fields.
        options.defaults = {
            k: v.default if isinstance(v, FieldDescriptor) else v
            for k, v in defaults.items()
            if k in fields
        }

        # extract all Model fields.
        options.models = {
            field: typ
            for field, typ in fields.items()
            if is_model(typ)
        }
        modelset = options.modelset = frozenset(options.models)

        # extract all fields that are not built-in types,
        # e.g. List[datetime]
        options.converse = {}
        if options.isodates:
            options.converse = {
                field: Converter(typ, cls._parse_iso8601)
                for field, typ in fields.items()
                if field not in modelset and is_date(typ)
            }

    @staticmethod
    def _parse_iso8601(typ: Type, data: Any) -> Optional[datetime]:
        if data is None:
            return None
        if isinstance(data, datetime):
            return data
        return iso8601.parse(data)

    @classmethod
    def _contribute_field_descriptors(
            cls,
            target: Type,
            options: ModelOptions,
            parent: FieldDescriptorT = None) -> None:
        fields = options.fields
        defaults = options.defaults
        for field, typ in fields.items():
            try:
                default, required = defaults[field], False
            except KeyError:
                default, required = None, True
            setattr(target, field, FieldDescriptor(
                field, typ, cls, required, default, parent))

    @classmethod
    def from_data(cls, data: Mapping) -> 'Record':
        # check for blessed key to see if another model should be used.
        self_cls = cls._maybe_namespace(data)
        return (self_cls or cls)(**data, __strict__=False)

    def __init__(self,
                 *args: Any,
                 __strict__: bool = True,
                 **kwargs: Any) -> None:
        # Set fields from keyword arguments.
        self._init_fields(args, kwargs, strict=__strict__)

    def _init_fields(self, positional: Tuple, keywords: Dict,
                     *,
                     strict: bool = True) -> None:
        fields = self._to_fieldmap(positional, keywords)
        fields.pop('__faust', None)  # remove metadata
        fieldset = frozenset(fields)
        options = self._options
        get_field = fields.get

        # Check all required arguments.
        missing = options.fieldset - fieldset - options.optionalset
        if missing:
            raise TypeError('{} missing required {}: {}'.format(
                type(self).__name__,
                pluralize(len(missing), 'argument'),
                ', '.join(sorted(missing))))

        if strict:
            # Check for unknown arguments.
            extraneous = fieldset - options.fieldset
            if extraneous:
                raise TypeError('{} got unexpected {}: {}'.format(
                    type(self).__name__,
                    pluralize(len(extraneous), 'argument'),
                    ', '.join(sorted(extraneous))))

        # Reconstruct child models
        fields.update({
            k: self._to_models(typ, get_field(k))
            for k, typ in self._options.models.items()
        })

        # Reconstruct non-builtin types
        fields.update({
            k: self._reconstruct_type(typ, get_field(k), callback)
            for k, (typ, callback) in self._options.converse.items()})

        # Fast: This sets attributes from kwargs.
        self.__dict__.update(fields)

    def _to_fieldmap(self, positional: Tuple, keywords: Dict) -> Dict:
        if positional:
            pos2field = self._options.fieldpos.__getitem__
            keywords.update({
                pos2field(i): arg
                for i, arg in enumerate(positional)
            })
        return keywords

    def _to_models(self, typ: Type[ModelT], data: Any) -> Any:
        # convert argument that is a submodel (can be List[X] or X)
        return self._reconstruct_type(typ, data, self._to_model)

    def _to_model(self, typ: Type[ModelT], data: Any) -> ModelT:
        # _to_models uses this as a callback to _reconstruct_type,
        # called everytime something needs to be converted into a model.
        if data is not None and not isinstance(data, typ):
            return typ.from_data(data)
        return data

    def _reconstruct_type(
            self, typ: Type, data: Any,
            callback: Callable[[Type, Any], Any]) -> Any:
        if data is not None:
            try:
                # Get generic type (if any)
                # E.g. Set[typ], List[typ], Optional[List[typ]] etc.
                generic, subtyp = guess_concrete_type(typ)
            except TypeError:
                # just a scalar
                return callback(typ, data)
            else:
                if generic is list:
                    return [callback(subtyp, v) for v in data]
                elif generic is tuple:
                    return tuple(callback(subtyp, v) for v in data)
                elif generic is dict:
                    return {k: callback(subtyp, v) for k, v in data.items()}
                elif generic is set:
                    return {callback(subtyp, v) for v in data}
        return data

    def _derive(self, *objects: ModelT, **fields: Any) -> ModelT:
        data = cast(Dict, self.to_representation())
        for obj in objects:
            data.update(cast(Record, obj).to_representation())
        return type(self)(**{**data, **fields})

    def to_representation(self) -> Mapping[str, Any]:
        # Convert known fields to mapping of ``{field: value}``.
        return dict(self._asitems())

    def _asitems(self) -> Iterable[Tuple[Any, Any]]:
        # Iterate over known fields as items-tuples.
        modelset = self._options.modelset
        for key in self._options.fields:
            value = getattr(self, key)
            if key in modelset and isinstance(value, ModelT):
                value = value.to_representation()
            yield key, value
        if self._options.include_metadata:
            yield '__faust', {'ns': self._options.namespace}

    def _humanize(self) -> str:
        # we try to preserve the order of fields specified in the class,
        # so doing {**self._options.defaults, **self.__dict__} does not work.
        attrs, defaults = self.__dict__, self._options.defaults.items()
        fields = {
            **attrs,
            **{k: v for k, v in defaults if k not in attrs},
        }
        return _kvrepr(fields)

    def __json__(self) -> Any:
        return self.to_representation()

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, type(self)):
            return all(
                getattr(self, key) == getattr(other, key)
                for key in self._options.fields
            )
        return False

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return object.__hash__(self)


def _kvrepr(d: Mapping[str, Any],
            *,
            sep: str = ', ') -> str:
    """Represent dict as `k='v'` pairs separated by comma."""
    return sep.join(f'{k}={v!r}' for k, v in d.items())
