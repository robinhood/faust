"""Record - Dictionary Model."""
from datetime import datetime
from decimal import Decimal
from itertools import chain
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Mapping,
    MutableMapping,
    Set,
    Tuple,
    Type,
    cast,
)

from mode.utils.compat import OrderedDict
from mode.utils.objects import (
    annotations,
    is_optional,
    remove_optional,
)
from mode.utils.text import pluralize

from faust.types.models import (
    CoercionMapping,
    FieldDescriptorT,
    FieldMap,
    IsInstanceArgT,
    ModelOptions,
    ModelT,
)
from faust.utils import codegen

from .base import Model
from .fields import FieldDescriptor, field_for_type
from .tags import Tag

__all__ = ['Record']

DATE_TYPES: IsInstanceArgT = (datetime,)
DECIMAL_TYPES: IsInstanceArgT = (Decimal,)

ALIAS_FIELD_TYPES = {
    dict: Dict,
    tuple: Tuple,
    list: List,
    set: Set,
    frozenset: FrozenSet,
}

E_NON_DEFAULT_FOLLOWS_DEFAULT = '''
Non-default {cls_name} field {field_name} cannot
follow default {fields} {default_names}
'''

_ReconFun = Callable[..., Any]


class Record(Model, abstract=True):  # type: ignore
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

    def __init_subclass__(cls,
                          serializer: str = None,
                          namespace: str = None,
                          include_metadata: bool = None,
                          isodates: bool = None,
                          abstract: bool = False,
                          allow_blessed_key: bool = None,
                          decimals: bool = None,
                          coerce: bool = None,
                          coercions: CoercionMapping = None,
                          polymorphic_fields: bool = None,
                          validation: bool = None,
                          date_parser: Callable[[Any], datetime] = None,
                          lazy_creation: bool = False,
                          **kwargs: Any) -> None:
        # XXX mypy 0.750 requires this to be defined on the class,
        # and do not recognize the parent class signature.

        super().__init_subclass__(
            serializer=serializer,
            namespace=namespace,
            include_metadata=include_metadata,
            isodates=isodates,
            abstract=abstract,
            allow_blessed_key=allow_blessed_key,
            decimals=decimals,
            coerce=coerce,
            coercions=coercions,
            polymorphic_fields=polymorphic_fields,
            validation=validation,
            date_parser=date_parser,
            lazy_creation=lazy_creation,
            **kwargs)

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
            localns={cls.__name__: cls},
        )
        options.fields = cast(Mapping, fields)
        options.fieldset = frozenset(fields)
        options.fieldpos = {i: k for i, k in enumerate(fields.keys())}

        # extract all default values, but only for actual fields.
        options.defaults = {
            k: v.default if isinstance(v, FieldDescriptor) else v
            for k, v in defaults.items()
            if k in fields and not (
                isinstance(v, FieldDescriptor) and v.required)
        }

        # Raise error if non-defaults are mixed in with defaults
        # like namedtuple/dataclasses do.
        local_defaults = []
        for attr_name in cls.__annotations__:
            if attr_name in cls.__dict__:
                default_value = cls.__dict__[attr_name]
                if isinstance(default_value, FieldDescriptorT):
                    if not default_value.required:
                        local_defaults.append(attr_name)
                else:
                    local_defaults.append(attr_name)
            else:
                if local_defaults:
                    raise TypeError(E_NON_DEFAULT_FOLLOWS_DEFAULT.format(
                        cls_name=cls.__name__,
                        field_name=attr_name,
                        fields=pluralize(len(local_defaults), 'field'),
                        default_names=', '.join(local_defaults),
                    ))

        for field, typ in fields.items():
            if is_optional(typ):
                # Optional[X] also needs to be added to defaults mapping.
                options.defaults.setdefault(field, None)

        # Create frozenset index of default fields.
        options.optionalset = frozenset(options.defaults)

    @classmethod
    def _contribute_methods(cls) -> None:
        if not getattr(cls.asdict, 'faust_generated', False):
            raise RuntimeError('Not allowed to override Record.asdict()')
        cls.asdict = cls._BUILD_asdict()  # type: ignore
        cls.asdict.faust_generated = True  # type: ignore

        cls._input_translate_fields = \
            cls._BUILD_input_translate_fields()

    @classmethod
    def _contribute_field_descriptors(
            cls,
            target: Type,
            options: ModelOptions,
            parent: FieldDescriptorT = None) -> FieldMap:
        fields = options.fields
        defaults = options.defaults
        date_parser = options.date_parser
        coerce = options.coerce
        index = {}

        secret_fields = set()
        sensitive_fields = set()
        personal_fields = set()
        tagged_fields = set()

        def add_to_tagged_indices(field: str, tag: Type[Tag]) -> None:
            if tag.is_secret:
                options.has_secret_fields = True
                secret_fields.add(field)
            if tag.is_sensitive:
                options.has_sensitive_fields = True
                sensitive_fields.add(field)
            if tag.is_personal:
                options.has_personal_fields = True
                personal_fields.add(field)
            options.has_tagged_fields = True
            tagged_fields.add(field)

        def add_related_to_tagged_indices(field: str,
                                          related_model: Type = None) -> None:
            if related_model is None:
                return
            try:
                related_options = related_model._options
            except AttributeError:
                return
            if related_options.has_secret_fields:
                options.has_secret_fields = True
                secret_fields.add(field)
            if related_options.has_sensitive_fields:
                options.has_sensitive_fields = True
                sensitive_fields.add(field)
            if related_options.has_personal_fields:
                options.has_personal_fields = True
                personal_fields.add(field)
            if related_options.has_tagged_fields:
                options.has_tagged_fields = True
                tagged_fields.add(field)

        for field, typ in fields.items():
            try:
                default, needed = defaults[field], False
            except KeyError:
                default, needed = None, True
            descr = getattr(target, field, None)
            if is_optional(typ):
                target_type = remove_optional(typ)
            else:
                target_type = typ
            if descr is None or not isinstance(descr, FieldDescriptorT):
                DescriptorType, tag = field_for_type(target_type)
                if tag:
                    add_to_tagged_indices(field, tag)
                descr = DescriptorType(
                    field=field,
                    type=typ,
                    model=cls,
                    required=needed,
                    default=default,
                    parent=parent,
                    coerce=coerce,
                    date_parser=date_parser,
                    tag=tag,
                )
            else:
                descr = descr.clone(
                    field=field,
                    type=typ,
                    model=cls,
                    required=needed,
                    default=default,
                    parent=parent,
                    coerce=coerce,
                )

            descr.on_model_attached()

            for related_model in descr.related_models:
                add_related_to_tagged_indices(field, related_model)
            setattr(target, field, descr)
            index[field] = descr

        options.secret_fields = frozenset(secret_fields)
        options.sensitive_fields = frozenset(sensitive_fields)
        options.personal_fields = frozenset(personal_fields)
        options.tagged_fields = frozenset(tagged_fields)
        return index

    @classmethod
    def from_data(cls, data: Mapping, *,
                  preferred_type: Type[ModelT] = None) -> 'Record':
        """Create model object from Python dictionary."""
        # check for blessed key to see if another model should be used.
        if hasattr(data, '__is_model__'):
            return cast(Record, data)
        else:
            self_cls = cls._maybe_namespace(
                data, preferred_type=preferred_type)
        cls._input_translate_fields(data)
        return (self_cls or cls)(**data, __strict__=False)

    def __init__(self, *args: Any,
                 __strict__: bool = True,
                 __faust: Any = None,
                 **kwargs: Any) -> None:  # pragma: no cover
        ...  # overridden by _BUILD_init

    @classmethod
    def _BUILD_input_translate_fields(cls) -> Callable[[MutableMapping], None]:
        translate = [
            f'data[{field!r}] = data.pop({d.input_name!r}, None)'
            for field, d in cls._options.descriptors.items()
            if d.field != d.input_name
        ]

        return cast(Callable, classmethod(codegen.Function(
            '_input_translate_fields',
            ['cls', 'data'],
            translate if translate else ['pass'],
            globals=globals(),
            locals=locals(),
        )))

    @classmethod
    def _BUILD_init(cls) -> Callable[[], None]:
        # generate init function that set field values from arguments,
        # and that load data from serialized form.
        #
        #
        # The general template that we will be generating is
        #
        #    def __outer__(Model):   # create __init__ closure
        #       __defaults__ = Model._options.defaults
        #       __descr__ = Model._options.descriptors
        #       {% for field in fields_with_defaults %}
        #       _default_{{ field }}_ = __defaults__["{{ field }}"]
        #       {% endfor %}
        #       {% for field in fields_with_init %}
        #       _init_{{ field }}_ = __descr__["{{ field }}"].to_python
        #
        #       def __init__(self, {{ sig }}, *, __strict__=True, **kwargs):
        #          self.__evaluated_fields__ = set()
        #          if __strict__:  # creating model from Python
        #              {% for field in fields %}
        #              self.{{ field }} = {{ field }}
        #              {% endfor %}
        #              if kwargs:
        #                 # raise error for additional arguments
        #          else:
        #              {% for field in fields %}
        #              {% if OPTIONAL_FIELD(field) %}
        #              if {{ field }} is not None:
        #                  self.{{ field }} = _init_{{ field }}({{ field }})
        #              else:
        #                  self.{{ field }} = _default_{{ field }}
        #              {% else %}
        #                  self.{{ field }} = _init_{{ field }}({{ field }}
        #              # any additional kwargs are added as fields
        #              # when loading from serialized data.
        #              self.__dict__.update(kwargs)
        #         self.__post_init__()
        #     return __init__
        #
        options = cls._options
        field_positions = options.fieldpos
        optional = options.optionalset
        needs_validation = options.validation
        descriptors = options.descriptors
        has_post_init = hasattr(cls, '__post_init__')

        closures: Dict[str, str] = {
            '__defaults__': 'Model._options.defaults',
            '__descr__': 'Model._options.descriptors',
        }

        kwonlyargs = ['*', '__strict__=True', '__faust=None', '**kwargs']
        # these are sets, but we care about order as we will
        # be generating signature arguments in the correct order.
        #
        # The order is decided by the order of fields in the class):
        #
        #  class Foo(Record):
        #      c: int
        #      a: int
        #
        # becomes:
        #
        #   def __init__(self, c, a):
        #       self.c = c
        #       self.a = a
        optional_fields: Dict[str, bool] = OrderedDict()
        required_fields: Dict[str, bool] = OrderedDict()

        def generate_setter(field: str, getval: str) -> str:
            """Generate code that sets attribute for field in class.

            Arguments:
                field: Name of field.
                getval: Source code that initializes value for field,
                    can be the field name itself for no initialization
                    or for example: ``f"self._prepare_value({field})"``.
                out: Destination list where new source code lines are added.
                """
            if field in optional:
                optional_fields[field] = True
                default_var = f'_default_{field}_'
                closures[default_var] = f'__defaults__["{field}"]'
                return (f'    self.{field} = {getval} '
                        f'if {field} is not None else {default_var}')
            else:
                required_fields[field] = True
                return f'    self.{field} = {getval}'

        def generate_prepare_value(field: str) -> str:
            descriptor = descriptors[field]
            if descriptor.lazy_coercion:
                return field  # no initialization
            else:
                # call descriptor.to_python
                init_field_var = f'_init_{field}_'
                closures[init_field_var] = f'__descr__["{field}"].to_python'
                return f'{init_field_var}({field})'

        preamble = [
            'self.__evaluated_fields__ = set()',
        ]

        data_setters = ['if __strict__:'] + [
            generate_setter(field, field)
            for field in field_positions.values()
        ]

        data_rest = [
            '    if kwargs:',
            '        from mode.utils.text import pluralize',
            '        message = "{} got unexpected {}: {}".format(',
            '            self.__class__.__name__,',
            '            pluralize(kwargs.__len__(), "argument"),',
            '            ", ".join(map(str, sorted(kwargs))))',
            '        raise TypeError(message)',
        ]

        init_setters = ['else:']
        if field_positions:
            init_setters.extend(
                generate_setter(field, generate_prepare_value(field))
                for field in field_positions.values()
            )
        init_setters.append('    self.__dict__.update(kwargs)')

        postamble = []
        if has_post_init:
            postamble.append('self.__post_init__()')
        if needs_validation:
            postamble.append('self.validate_or_raise()')

        signature = list(chain(
            ['self'],
            [f'{field}' for field in required_fields],
            [f'{field}=None' for field in optional_fields],
            kwonlyargs,
        ))

        sourcecode = codegen.build_closure_source(
            name='__init__',
            args=signature,
            body=list(chain(
                preamble,
                data_setters,
                data_rest,
                init_setters,
                postamble,
            )),
            closures=closures,
            outer_args=['Model'],
        )

        # TIP final sourcecode also available
        # as .__sourcecode__ on returned method
        # (print(Model.__init__.__sourcecode__)
        return codegen.build_closure(
            '__outer__', sourcecode, cls,
            globals={},
            locals={},
        )

    @classmethod
    def _BUILD_hash(cls) -> Callable[[], None]:
        return codegen.HashMethod(list(cls._options.fields),
                                  globals=globals(),
                                  locals=locals())

    @classmethod
    def _BUILD_eq(cls) -> Callable[[], None]:
        return codegen.EqMethod(list(cls._options.fields),
                                globals=globals(),
                                locals=locals())

    @classmethod
    def _BUILD_ne(cls) -> Callable[[], None]:
        return codegen.NeMethod(list(cls._options.fields),
                                globals=globals(),
                                locals=locals())

    @classmethod
    def _BUILD_gt(cls) -> Callable[[], None]:
        return codegen.GtMethod(list(cls._options.fields),
                                globals=globals(),
                                locals=locals())

    @classmethod
    def _BUILD_ge(cls) -> Callable[[], None]:
        return codegen.GeMethod(list(cls._options.fields),
                                globals=globals(),
                                locals=locals())

    @classmethod
    def _BUILD_lt(cls) -> Callable[[], None]:
        return codegen.LtMethod(list(cls._options.fields),
                                globals=globals(),
                                locals=locals())

    @classmethod
    def _BUILD_le(cls) -> Callable[[], None]:
        return codegen.LeMethod(list(cls._options.fields),
                                globals=globals(),
                                locals=locals())

    @classmethod
    def _BUILD_asdict(cls) -> Callable[..., Dict[str, Any]]:
        preamble = [
            'return self._prepare_dict({',
        ]

        fields = [
            f'  {d.output_name!r}: {cls._BUILD_asdict_field(name, d)},'
            for name, d in cls._options.descriptors.items()
            if not d.exclude
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
    def _BUILD_asdict_field(cls, name: str, field: FieldDescriptorT) -> str:
        return f'self.{name}'

    def _derive(self, *objects: ModelT, **fields: Any) -> ModelT:
        data = self.asdict()
        for obj in objects:
            data.update(cast(Record, obj).asdict())
        return type(self)(**{**data, **fields})

    def to_representation(self) -> Mapping[str, Any]:
        """Convert model to its Python generic counterpart.

        Records will be converted to dictionary.
        """
        # Convert known fields to mapping of ``{field: value}``.
        payload = self.asdict()
        options = self._options
        if options.include_metadata:
            payload['__faust'] = {'ns': options.namespace}
        return payload

    def asdict(self) -> Dict[str, Any]:  # pragma: no cover
        """Convert record to Python dictionary."""
        ...  # generated by _BUILD_asdict
    # Used to disallow overriding this method
    asdict.faust_generated = True  # type: ignore

    def _humanize(self) -> str:
        # we try to preserve the order of fields specified in the class,
        # so doing {**self._options.defaults, **self.__dict__} does not work.
        attrs, defaults = self.__dict__, self._options.defaults.items()
        fields = {
            **{k: v for k, v in attrs.items() if not k.startswith('__')},
            **{k: v
               for k, v in defaults if k not in attrs},
        }
        return _kvrepr(fields)

    def __json__(self) -> Any:
        return self.to_representation()

    def __eq__(self, other: Any) -> bool:  # pragma: no cover
        # implemented by BUILD_eq
        return NotImplemented

    def __ne__(self, other: Any) -> bool:  # pragma: no cover
        # implemented by BUILD_ne
        return NotImplemented

    def __lt__(self, other: 'Record') -> bool:  # pragma: no cover
        # implemented by BUILD_lt
        return NotImplemented

    def __le__(self, other: 'Record') -> bool:  # pragma: no cover
        # implemented by BUILD_le
        return NotImplemented

    def __gt__(self, other: 'Record') -> bool:  # pragma: no cover
        # implemented by BUILD_gt
        return NotImplemented

    def __ge__(self, other: 'Record') -> bool:  # pragma: no cover
        # implemented by BUILD_ge
        return NotImplemented


def _kvrepr(d: Mapping[str, Any], *, sep: str = ', ') -> str:
    """Represent dict as `k='v'` pairs separated by comma."""
    return sep.join(f'{k}={v!r}' for k, v in d.items())
