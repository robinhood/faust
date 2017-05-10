from typing import (
    Any, ClassVar, Dict, Iterable, Mapping, Set, Sequence, Tuple, cast,
)
from ..serializers.avro import to_avro_type
from ..types.models import ModelT, ModelOptions
from ..types.tuples import Request
from ..utils.objects import annotations
from .base import FieldDescriptor, Model

__all__ = ['Record']


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
        # Find attributes and their types, and create indexes for these
        # for performance at runtime.
        fields, defaults = annotations(cls, stop=Record)
        options.fields = cast(Mapping, fields)
        options.fieldset = frozenset(fields)
        options.optionalset = frozenset(defaults)
        # extract all default values, but only for actual fields.
        options.defaults = {
            k: v for k, v in defaults.items()
            if k in fields
        }
        options.models = {
            field: typ for field, typ in fields.items()
            if issubclass(typ, ModelT)
        }
        options.modelset = frozenset(options.models)

    @classmethod
    def _contribute_field_descriptors(cls, options: ModelOptions) -> None:
        fields = options.fields
        defaults = options.defaults
        for field, typ in fields.items():
            try:
                default, required = defaults[field], False
            except KeyError:
                default, required = None, True
            setattr(cls, field, FieldDescriptor(
                field, typ, cls, required, default))

    def __init__(self, _data: Any = None,
                 *,
                 req: Request = None,
                 **fields: Any) -> None:
        # Req is only set by the Consumer, when the record
        # originates from a message.
        self.req = req

        if _data is not None:
            assert not fields
            self._init_fields(_data)
        else:
            # Set fields from keyword arguments.
            self._init_fields(fields)

    def _init_fields(self, fields: Mapping) -> None:
        fieldset = frozenset(fields)
        options = self._options

        # Check all required arguments.
        missing = options.fieldset - fieldset - options.optionalset
        if missing:
            raise TypeError('{} missing required arguments: {}'.format(
                type(self).__name__, ', '.join(sorted(missing))))

        # Check for unknown arguments.
        extraneous = fieldset - options.fieldset
        if extraneous:
            raise TypeError('{} got unexpected arguments: {}'.format(
                type(self).__name__, ', '.join(sorted(extraneous))))

        # Fast: This sets attributes from kwargs.
        self.__dict__.update(fields)

        # then reconstruct child models
        for _field, _typ in self._options.models.items():
            _data = fields.get(_field)
            if _data is not None and not isinstance(_data, ModelT):
                _data = _typ(_data)
            self.__dict__[_field] = _data

    def _derive(self, objects: Tuple[ModelT, ...], fields: Dict) -> ModelT:
        data = cast(Dict, self.to_representation())
        for obj in objects:
            data.update(cast(Record, obj).to_representation())
        return type(self)(req=self.req, **{**data, **fields})

    def to_representation(self) -> Mapping[str, Any]:
        # Convert known fields to mapping of ``{field: value}``.
        return dict(self._asitems())

    def _asitems(self) -> Iterable[Tuple[Any, Any]]:
        # Iterate over known fields as items-tuples.
        modelset = self._options.modelset
        for key in self._options.fields:
            value = getattr(self, key)
            if key in modelset and value is not None:
                value = value.to_representation()
            yield key, value

    def _humanize(self) -> str:
        # we try to preserve the order of fields specified in the class,
        # so doing {**self._options.defaults, **self.__dict__} does not work.
        attrs, defaults = self.__dict__, self._options.defaults.items()
        fields = {
            **attrs,
            **{k: v for k, v in defaults if k not in attrs},
        }
        return _kvrepr(fields, skip={'req'})

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, type(self)):
            return all(
                getattr(self, key) == getattr(other, key)
                for key in self._options.fields
            )
        return False

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)


def _kvrepr(d: Mapping[str, Any],
            *,
            skip: Set[str] = None,
            sep: str = ', ',
            fmt: str = '{0}={1!r}') -> str:
    """Represent dict as `k='v'` pairs separated by comma."""
    skip = skip or set()
    return sep.join(
        fmt.format(k, v) for k, v in d.items()
        if k not in skip
    )
