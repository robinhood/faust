"""Record - Dictionary Model."""
from datetime import datetime
from functools import partial
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    cast,
)

from mode.utils.objects import annotations, guess_concrete_type

from faust.types.models import (
    Converter,
    FieldDescriptorT,
    ModelOptions,
    ModelT,
)
from faust.utils import codegen
from faust.utils import iso8601

from .base import FieldDescriptor, Model

__all__ = ['Record']

DATE_TYPES = (datetime,)

ALIAS_FIELD_TYPES = {
    dict: Dict,
    tuple: Tuple,
    list: List,
    set: Set,
    frozenset: FrozenSet,
}

_ReconFun = Callable[[Type, Any], Any]

# Models can refer to other models:
#
#   class M(Model):
#     x: OtherModel
#
# but can also have List-of-X, Mapping-of-X, etc:
#
#  class M(Model):
#    x: List[OtherModel]
#    y: Mapping[KeyModel, ValueModel]
#
# in the source code we refer to a concrete type, in the example above
# the concrete type for x would be `list`, and the concrete type
# for y would be `dict`.

__concrete_type_cache: Dict[Type, Tuple[Type, Type]] = {}


def _concrete_type(typ: Type) -> Tuple[Type, Type]:
    try:
        concrete_type, cls = __concrete_type_cache[typ]
    except KeyError:
        try:
            val = guess_concrete_type(typ)
        except TypeError:
            val = (TypeError, None)
        __concrete_type_cache[typ] = val
        return val
    if concrete_type is TypeError:
        raise TypeError()
    return concrete_type, cls


def _is_model(cls: Type) -> Tuple[bool, Optional[Type]]:
    # Returns (is_model, concrete_type).
    #  concrete type (if available) will be list if it's a list, dict if dict,
    #  etc, then that means it's a List[ModelType], Dict[ModelType] etc, so
    # we have to deserialize them as such.
    concrete_type = None
    try:
        concrete_type, cls = guess_concrete_type(cls)
    except TypeError:
        pass
    try:
        return issubclass(cls, ModelT), concrete_type
    except TypeError:  # typing.Any cannot be used with subclass
        return False, None


def _is_date(cls: Type, *, types: Tuple[Type, ...] = DATE_TYPES) -> bool:
    try:
        # Check for List[int], Mapping[int, int], etc.
        _, cls = guess_concrete_type(cls)
    except TypeError:
        pass
    try:
        return issubclass(cls, types)
    except TypeError:
        return False


def _field_callback(typ: Type, callback: _ReconFun) -> Any:
    try:
        generic, subtyp = _concrete_type(typ)
    except TypeError:
        pass
    else:
        if generic is list:
            return partial(_from_generic_list, subtyp, callback)
        elif generic is tuple:
            return partial(_from_generic_tuple, subtyp, callback)
        elif generic is dict:
            return partial(_from_generic_dict, subtyp, callback)
        elif generic is set:
            return partial(_from_generic_set, subtyp, callback)
    return partial(callback, typ)


def _from_generic_list(typ: Type, callback: _ReconFun, data: Iterable) -> List:
    return [callback(typ, v) for v in data]


def _from_generic_tuple(typ: Type, callback: _ReconFun, data: Tuple) -> Tuple:
    return tuple(callback(typ, v) for v in data)


def _from_generic_dict(typ: Type,
                       callback: _ReconFun, data: Mapping) -> Mapping:
    return {k: callback(typ, v) for k, v in data.items()}


def _from_generic_set(typ: Type, callback: _ReconFun, data: Set) -> Set:
    return {callback(typ, v) for v in data}


def _to_model(typ: Type[ModelT], data: Any) -> Optional[ModelT]:
    # called everytime something needs to be converted into a model.
    if data is not None and not isinstance(data, typ):
        model = typ.from_data(data, preferred_type=typ)
        return model if model is not None else data
    return data


def _maybe_to_representation(val: ModelT = None) -> Optional[Any]:
    return val.to_representation() if val is not None else None


class Record(Model, abstract=True):
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

    @classmethod
    def _contribute_to_options(cls, options: ModelOptions) -> None:
        # Find attributes and their types, and create indexes for these.
        # This only happens once when the class is created, so Faust
        # models are fast at runtime.
        fields, defaults = annotations(
            cls,
            stop=Record,
            skip_classvar=True,
            alias_types=ALIAS_FIELD_TYPES,
        )
        options.fields = cast(Mapping, fields)
        options.fieldset = frozenset(fields)
        options.fieldpos = {i: k for i, k in enumerate(fields.keys())}
        is_date = _is_date

        # extract all default values, but only for actual fields.
        options.defaults = {
            k: v.default if isinstance(v, FieldDescriptor) else v
            for k, v in defaults.items()
            if k in fields and not (
                isinstance(v, FieldDescriptor) and v.required)
        }
        options.optionalset = frozenset(options.defaults)

        options.models = {}
        modelattrs = options.modelattrs = {}

        for field, typ in fields.items():
            is_model, concrete_type = _is_model(typ)
            if is_model:
                # Extract all model fields
                options.models[field] = typ
                # Create mapping of model fields to concrete types if available
                modelattrs[field] = concrete_type

        # extract all fields that are not built-in types,
        # e.g. List[datetime]
        options.converse = {}
        if options.isodates:
            options.converse = {
                field: Converter(typ, cls._parse_iso8601)
                for field, typ in fields.items()
                if field not in modelattrs and is_date(typ)
            }

    @classmethod
    def _contribute_methods(cls) -> None:
        if not getattr(cls.asdict, 'faust_generated', False):
            raise RuntimeError('Not allowed to override Record.asdict()')
        cls.asdict = cls._BUILD_asdict()
        cls.asdict.faust_generated = True

    @staticmethod
    def _parse_iso8601(typ: Type, data: Any) -> Optional[datetime]:
        if data is None:
            return None
        if isinstance(data, datetime):
            return data
        return iso8601.parse(data)

    @classmethod
    def _contribute_field_descriptors(cls,
                                      target: Type,
                                      options: ModelOptions,
                                      parent: FieldDescriptorT = None) -> None:
        fields = options.fields
        defaults = options.defaults
        for field, typ in fields.items():
            try:
                default, needed = defaults[field], False
            except KeyError:
                default, needed = None, True
            setattr(target, field,
                    FieldDescriptor(field, typ, cls, needed, default, parent))

    @classmethod
    def from_data(cls, data: Mapping, *,
                  preferred_type: Type[ModelT] = None) -> 'Record':
        # check for blessed key to see if another model should be used.
        if hasattr(data, '__is_model__'):
            return cast(Record, data)
        else:
            self_cls = cls._maybe_namespace(
                data, preferred_type=preferred_type)
        return (self_cls or cls)(**data, __strict__=False)

    def __init__(self, *args: Any,
                 __strict__: bool = True,
                 __faust: Any = None,
                 **kwargs: Any) -> None:  # pragma: no cover
        ...  # overridden by _BUILD_init

    @classmethod
    def _BUILD_init(cls) -> Callable[[], None]:
        kwonlyargs = ['*', '__strict__=True', '__faust=None', '**kwargs']
        fields = cls._options.fieldpos
        optional = cls._options.optionalset
        models = cls._options.models
        converse = cls._options.converse
        initfield = cls._options.initfield = {}
        has_post_init = hasattr(cls, '__post_init__')
        required = []
        opts = []
        setters = []
        for field in fields.values():
            model = models.get(field)
            conv = converse.get(field)
            fieldval = f'{field}'
            if model is not None:
                initfield[field] = _field_callback(model, _to_model)
                assert initfield[field] is not None
                fieldval = f'self._init_field("{field}", {field})'
            if conv is not None:
                initfield[field] = _field_callback(*conv)
                assert initfield[field] is not None
                fieldval = f'self._init_field("{field}", {field})'
            if field in optional:
                opts.append(f'{field}=None')
                setters.extend([
                    f'if {field} is not None:',
                    f'  self.{field} = {fieldval}',
                    f'else:',
                    f'  self.{field} = self._options.defaults["{field}"]',
                ])
            else:
                required.append(field)
                setters.append(f'self.{field} = {fieldval}')

        rest = [
            'if kwargs and __strict__:',
            '    from mode.utils.text import pluralize',
            '    raise TypeError("{} got unexpected {}: {}".format(',
            '        type(self).__name__,',
            '        pluralize(len(kwargs), "argument"),',
            '        ", ".join(sorted(kwargs))))',
            'self.__dict__.update(kwargs)',
        ]

        if has_post_init:
            rest.extend([
                'self.__post_init__()',
            ])

        return codegen.InitMethod(
            required + opts + kwonlyargs,
            setters + rest,
            globals=globals(),
            locals=locals(),
        )

    def _init_field(self, field: str, value: Any) -> Any:
        return self._options.initfield[field](value)

    @classmethod
    def _BUILD_asdict(cls) -> Callable[..., Dict[str, Any]]:
        preamble = [
            'return self._prepare_dict({',
        ]

        fields = [
            f'  {field!r}: {cls._BUILD_asdict_field(field)},'
            for field in cls._options.fields
        ]

        postamble = [
            '})',
        ]

        return codegen.Method(
            '_asdict',
            [],
            preamble + fields + postamble,
            globals=globals(),
            locals=locals(),
        )

    def _prepare_dict(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return payload

    @classmethod
    def _BUILD_asdict_field(cls, field: str) -> str:
        modelattrs = cls._options.modelattrs
        is_model = field in modelattrs
        if is_model:
            generic = modelattrs[field]
            if generic is list or generic is tuple:
                return f'[v.to_representation() for v in self.{field}]'
            elif generic is set:
                return f'self.{field}'
            elif generic is dict:
                return (f'{{k: v.to_representation() '
                        f'  for k, v in self.{field}.items()}}')
            else:
                return f'_maybe_to_representation(self.{field})'
        else:
            return f'self.{field}'

    def _derive(self, *objects: ModelT, **fields: Any) -> ModelT:
        data = self.asdict()
        for obj in objects:
            data.update(cast(Record, obj).asdict())
        return type(self)(**{**data, **fields})

    def to_representation(self) -> Mapping[str, Any]:
        # Convert known fields to mapping of ``{field: value}``.
        payload = self.asdict()
        if self._options.include_metadata:
            payload['__faust'] = {'ns': self._options.namespace}
        return payload

    def asdict(self) -> Dict[str, Any]:  # pragma: no cover
        ...  # generated by _BUILD_asdict
    # Used to disallow overriding this method
    asdict.faust_generated = True  # type: ignore

    def _humanize(self) -> str:
        # we try to preserve the order of fields specified in the class,
        # so doing {**self._options.defaults, **self.__dict__} does not work.
        attrs, defaults = self.__dict__, self._options.defaults.items()
        fields = {
            **attrs,
            **{k: v
               for k, v in defaults if k not in attrs},
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


def _kvrepr(d: Mapping[str, Any], *, sep: str = ', ') -> str:
    """Represent dict as `k='v'` pairs separated by comma."""
    return sep.join(f'{k}={v!r}' for k, v in d.items())
