import abc
import logging
import ssl
import typing
import warnings
from datetime import timezone, tzinfo
from pathlib import Path as _Path
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    List,
    Mapping,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

from mode import Seconds as _Seconds, want_seconds
from mode.utils.imports import SymbolArg, symbol_by_name
from mode.utils.logging import Severity as _Severity
from yarl import URL as _URL

from faust.exceptions import ImproperlyConfigured
from faust.utils import json
from faust.utils.urls import URIListArg, urllist

from faust.types.auth import CredentialsArg, CredentialsT, to_credentials
from faust.types.codecs import CodecArg

if typing.TYPE_CHECKING:
    from .settings import Settings as _Settings
    from .sections import Section as _Section
else:
    class _Section: ...       # noqa
    class _Settings: ...      # noqa

__all__ = [
    'AutodiscoverArg',
    'DictArg',
    'URLArg',
    'BrokerArg',
    'Param',
    'Bool',
    'Str',
    'Severity',
    'Int',
    'UnsignedInt',
    'Version',
    'Port',
    'Seconds',
    'Credentials',
    'SSLContext',
    'Dict',
    'LogHandlers',
    'Timezone',
    'BrokerList',
    'URL',
    'Path',
    'Codec',
    'Enum',
    'Symbol',
    'to_bool',
    'setting',
]

#: Default transport used when no scheme specified.
DEFAULT_BROKER_SCHEME = 'kafka'

T = TypeVar('T')
IT = TypeVar('IT')   # Input type.
OT = TypeVar('OT')   # Output type.

BOOLEAN_TERMS: Mapping[str, bool] = {
    '': False,
    'false': False,
    'no': False,
    '0': False,
    'true': True,
    'yes': True,
    '1': True,
    'on': True,
    'off': False,
}

AutodiscoverArg = Union[
    bool,
    Iterable[str],
    Callable[[], Iterable[str]],
]

DictArg = Union[str, Mapping[str, T]]

URLArg = Union[str, _URL]
BrokerArg = URIListArg

DEPRECATION_WARNING_TEMPLATE = '''
Setting {self.name} is deprecated since Faust version \
{self.version_deprecated}: {self.deprecation_reason}. {alt_removal}
'''.strip()

DEPRECATION_REMOVAL_WARNING = '''
Further the setting is scheduled to be removed in Faust version \
{self.version_removal}.
'''.strip()


def to_bool(term: Union[str, bool], *,
            table: Mapping[str, bool] = BOOLEAN_TERMS) -> bool:
    """Convert common terms for true/false to bool.

    Examples (true/false/yes/no/on/off/1/0).
    """
    if table is None:
        table = BOOLEAN_TERMS
    if isinstance(term, str):
        try:
            return table[term.lower()]
        except KeyError:
            raise TypeError('Cannot coerce {0!r} to type bool'.format(term))
    return term


OutputCallable = Callable[[_Settings, OT], OT]
OnDefaultCallable = Callable[[_Settings], IT]


class Param(Generic[IT, OT], property):
    text_type: str = ':class:`~typing.Any`'

    name: str
    storage_name: str

    default: IT = cast(IT, None)

    env_name: Optional[str] = None
    #: If setting is not customized this is an optional
    #: list of other settings that we should take default value from.
    #: For example the `broker_consumer`/`broker_producer` settings
    #: can configure the broker URL for consumers and producers separately
    #: but take their default value from the `broker` setting.
    default_alias: Optional[str] = None
    default_template: Optional[str] = None
    allow_none: bool = False
    # If set to True we don't modify the value
    # of the attribute to set a default.
    # This is used by e.g. the env_prefix setting
    # which has custom constructor code in Settings.on_init
    ignore_default: bool = False

    section: _Section
    version_introduced: Optional[str] = None
    version_deprecated: Optional[str] = None
    version_removed: Optional[str] = None
    version_changed: Optional[Mapping[str, str]] = None
    deprecation_reason: Optional[str] = None

    related_cli_options: Mapping[str, List[str]]
    related_settings: List[str]

    deprecation_warning_template: str = DEPRECATION_WARNING_TEMPLATE
    deprecation_removal_warning: str = DEPRECATION_REMOVAL_WARNING

    def __init__(self, *,
                 name: str,
                 env_name: str = None,
                 default: IT = None,
                 default_alias: str = None,
                 default_template: str = None,
                 allow_none: bool = None,
                 ignore_default: bool = None,
                 section: _Section = None,
                 version_introduced: str = None,
                 version_deprecated: str = None,
                 version_removed: str = None,
                 version_changed: Mapping[str, str] = None,
                 deprecation_reason: str = None,
                 related_cli_options: Mapping[str, List[str]] = None,
                 related_settings: List[str] = None,
                 help: str = None,
                 **kwargs: Any) -> None:
        assert name
        self.name = name
        self.storage_name = f'_{name}'
        if env_name is not None:
            self.env_name = env_name
        if default is not None:
            self.default = default
        if default_alias is not None:
            self.default_alias = default_alias
        if default_template is not None:
            self.default_template = default_template
        if allow_none is not None:
            self.allow_none = allow_none
        if ignore_default is not None:
            self.ignore_default = ignore_default
        if section is not None:
            self.section = section
        assert self.section
        if version_introduced is not None:
            self.version_introduced = version_introduced
        if version_deprecated is not None:
            self.version_deprecated = version_deprecated
        if version_removed is not None:
            self.version_removed = version_removed
        if version_changed is not None:
            self.version_changed = version_changed
        if deprecation_reason is not None:
            self.deprecation_reason = deprecation_reason
        if help is not None:
            self.__doc__ = help
        self._on_get_value_: Optional[OutputCallable] = None
        self._on_set_default_: Optional[OnDefaultCallable] = None
        self.options = kwargs
        self.related_cli_options = related_cli_options or {}
        self.related_settings = related_settings or []
        self._init_options(**self.options)

        if self.version_deprecated:
            assert self.deprecation_reason

    def _init_options(self, **kwargs: Any) -> None:
        ...

    def on_get_value(self, fun: OutputCallable) -> OutputCallable:
        assert self._on_get_value_ is None
        self._on_get_value_ = fun
        return fun

    def on_set_default(self, fun: OnDefaultCallable) -> OnDefaultCallable:
        assert self._on_set_default_ is None
        self._on_set_default_ = fun
        return fun

    def __get__(self, obj: Any, type: Type = None) -> OT:
        if obj is None:
            return self  # type: ignore
        if self.version_deprecated:
            # we use UserWarning because DeprecationWarning is silenced
            # by default in Python.
            warnings.warn(UserWarning(self.build_deprecation_warning()))
        return self.on_get(obj)

    def __set__(self, obj: Any, value: IT) -> None:
        self.on_set(obj, self.prepare_set(obj, value))

    def on_get(self, conf: _Settings) -> OT:
        value = getattr(conf, self.storage_name)
        if value is None and self.default_alias:
            retval = getattr(conf, self.default_alias)
        else:
            retval = self.prepare_get(conf, value)
        if self._on_get_value_ is not None:
            return self._on_get_value_(conf, retval)
        return retval

    def prepare_get(self, conf: _Settings, value: OT) -> OT:
        return value

    def on_set(self, settings: Any, value: OT) -> None:
        settings.__dict__[self.storage_name] = value
        assert getattr(settings, self.storage_name) == value

    def set_class_default(self, cls: Type) -> None:
        setattr(cls, self.storage_name, self.default)

    def on_init_set_value(self,
                          conf: _Settings,
                          provided_value: Optional[IT]) -> None:
        if provided_value is not None:
            self.__set__(conf, provided_value)

    def on_init_set_default(self,
                            conf: _Settings,
                            provided_value: Optional[IT]) -> None:
        if provided_value is None:
            default_value = self.default
            if self._on_set_default_:
                default_value = self._on_set_default_(conf)
            if default_value is None and self.default_template:
                default_value = self.default_template.format(conf=conf)
            setattr(conf, self.storage_name,
                    self.prepare_init_default(conf, default_value))

    def build_deprecation_warning(self) -> str:
        alt_removal = ''
        if self.version_removed:
            alt_removal = self.deprecation_removal_warning.format(self=self)
        return self.deprecation_warning_template.format(
            self=self,
            alt_removal=alt_removal,
        )

    def validate_before(self, value: IT = None) -> None:
        ...

    def validate_after(self, value: OT) -> None:
        ...

    def prepare_set(self, conf: _Settings, value: IT) -> OT:
        skip_validate = value is None and self.allow_none
        if not skip_validate:
            self.validate_before(value)
        if value is not None:
            new_value = self.to_python(conf, value)
        else:
            new_value = value
        if not skip_validate:
            self.validate_after(new_value)
        return new_value

    def prepare_init_default(self, conf: _Settings, value: IT) -> OT:
        if value is not None:
            return self.to_python(conf, value)
        return None

    def to_python(self, conf: _Settings, value: IT) -> OT:
        return cast(OT, value)

    @property
    def active(self) -> bool:
        return not bool(self.version_removed)

    @property
    def deprecated(self) -> bool:
        return bool(self.version_deprecated)

    @property
    def class_name(self) -> str:
        return type(self).__name__


class Bool(Param[Any, bool]):
    text_type = ':class:`bool`'

    def to_python(self, conf: _Settings, value: Any) -> bool:
        if isinstance(value, str):
            return to_bool(value)
        return bool(value)


class Str(Param[str, str]):
    text_type = ':class:`str`'


class Severity(Param[_Severity, _Severity]):
    text_type = ':class:`str` / :class:`int`'


class Number(Param[IT, OT]):
    min_value: Optional[int] = None
    max_value: Optional[int] = None

    def _init_options(self,
                      min_value: int = None,
                      max_value: int = None,
                      **kwargs: Any) -> None:
        if min_value is not None:
            self.min_value = min_value
        if max_value is not None:
            self.max_value = max_value

    @abc.abstractmethod
    def convert(self, conf: _Settings, value: IT) -> OT:
        ...

    def to_python(self,
                  conf: _Settings,
                  value: IT) -> OT:
        return self.convert(conf, value)

    def validate_after(self, value: OT) -> None:
        v = cast(int, value)
        min_ = self.min_value
        max_ = self.max_value
        if min_ is not None and v < min_:
            raise self._out_of_range(v)
        if max_ is not None and v > max_:
            raise self._out_of_range(v)

    def _out_of_range(self, value: float) -> ImproperlyConfigured:
        return ImproperlyConfigured(
            f'Value {value} is out of range for {self.class_name} '
            f'(min={self.min_value} max={self.max_value})')


NumberInputArg = Union[str, int, float]


class _Int(Number[IT, OT]):
    text_type = ':class:`int`'

    def convert(self,
                conf: _Settings,
                value: IT) -> OT:
        return cast(OT, int(cast(int, value)))


class Int(_Int[NumberInputArg, int]):
    ...


class UnsignedInt(_Int[NumberInputArg, int]):
    min_value = 0


class Version(Int):
    min_value = 1


class Port(UnsignedInt):
    max_value = 65535


class Seconds(Param[_Seconds, float]):
    text_type = ':class:`float` / :class:`~datetime.timedelta`'

    def to_python(self, conf: _Settings, value: _Seconds) -> float:
        return want_seconds(value)


class Credentials(Param[CredentialsArg, Optional[CredentialsT]]):
    text_type = ':class:`~faust.types.auth.CredentialsT`'

    def to_python(self,
                  conf: _Settings,
                  value: CredentialsArg) -> Optional[CredentialsT]:
        return to_credentials(value)


class SSLContext(Param[ssl.SSLContext, Optional[ssl.SSLContext]]):
    text_type = ':class:`ssl.SSLContext`'


class Dict(Param[DictArg[T], Mapping[str, T]]):
    text_type = ':class:`dict`'

    def to_python(self,
                  conf: _Settings,
                  value: DictArg[T]) -> Mapping[str, T]:
        if isinstance(value, str):
            return json.loads(value)
        elif isinstance(value, Mapping):
            return value
        return dict(value)


class LogHandlers(Param[List[logging.Handler], List[logging.Handler]]):
    text_type = '``List[logging.Handler]``'

    def prepare_init_default(
            self, conf: _Settings, value: Any) -> List[logging.Handler]:
        return []


class Timezone(Param[Union[str, tzinfo], tzinfo]):
    text_type = ':class:`datetime.tzinfo`'
    builtin_timezones = {'UTC': timezone.utc}

    def to_python(self, conf: _Settings, value: Union[str, tzinfo]) -> tzinfo:
        if isinstance(value, str):
            try:
                return cast(tzinfo, self.builtin_timezones[value.lower()])
            except KeyError:
                import pytz
                return cast(tzinfo, pytz.timezone(value))
        else:
            return value


class BrokerList(Param[BrokerArg, List[_URL]]):
    text_type = ':class:`str` / :class:`~yarl.URL` / ``List[URL/str]``'
    default_scheme = DEFAULT_BROKER_SCHEME

    def to_python(self, conf: _Settings, value: BrokerArg) -> List[_URL]:
        return self.broker_list(value)

    def broker_list(self, value: BrokerArg) -> List[_URL]:
        return urllist(value, default_scheme=self.default_scheme)


class URL(Param[URLArg, _URL]):
    text_type = ':class:`str` / :class:`~yarl.URL`'

    def to_python(self, conf: _Settings, value: URLArg) -> _URL:
        return _URL(value)


class Path(Param[Union[str, _Path], _Path]):
    text_type = ':class:`str` / :class:`~pathlib.Path`'
    expanduser: bool = True

    def to_python(self, conf: _Settings, value: Union[str, _Path]) -> _Path:
        p = _Path(value)
        if self.expanduser:
            p = p.expanduser()
        return self.prepare_path(conf, p)

    def prepare_path(self, conf: _Settings, path: _Path) -> _Path:
        return path


class Codec(Param[CodecArg, CodecArg]):
    text_type = ':class:`str` / :class:`~faust.serializers.codecs.Codec`'


def Enum(typ: T) -> Type[Param[Union[str, T], T]]:

    class EnumParam(Param[Union[str, T], T]):
        text_type = ':class:`str`'

        def to_python(self, conf: _Settings, value: Union[str, T]) -> T:
            return typ(value)  # type: ignore

    return EnumParam


class _Symbol(Param[IT, OT]):
    text_type = ':class:`str` / :class:`typing.Type`'

    def to_python(self, conf: _Settings, value: IT) -> OT:
        return cast(OT, symbol_by_name(value))


def Symbol(typ: T) -> Type[Param[SymbolArg[T], T]]:
    return _Symbol[SymbolArg[T], T]


def setting(param: Type[Param[IT, OT]]) -> OT:
    return cast(OT, param)
