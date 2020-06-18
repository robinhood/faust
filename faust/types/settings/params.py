import abc
import logging
import ssl
import typing
import warnings
from datetime import timedelta, timezone, tzinfo
from pathlib import Path as _Path
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
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
from faust.types.codecs import CodecArg, CodecT

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
    """Faust setting desscription.

    Describes a Faust setting, how to read it from environment
    variables or from a configuration object.

    """

    #: Textual description of setting type.
    #: This is used by :file:`extra/tools/render_configuration_reference.py`
    #: to display the types supported by this setting.
    #:
    #: Can be a tuple of actual classes, or a tuple of strings.
    #: If a tuple of classes say a setting that accepts :class:`str`
    #: and :class:`int`:
    #:
    #: .. sourcecode:: python
    #:
    #:      text_type = (str, int)
    #:
    #: the generated description will be:
    #:
    #: .. sourcecode:: restructuredtext
    #:
    #:   :type: :class:`str` / :class:`int`
    #:
    text_type: ClassVar[Tuple[Any, ...]] = (Any,)

    #: Setting name (e.g. ``broker_request_timeout``).
    name: str

    #: Storage name (e.g. ``_broker_request_timeout``).
    #: This is the attribute name where we'll be storing
    #: the actual value for the setting.
    #: For example ``Settings.broker_request_timeout`` will be a
    #: property that calls ``Param.__get__`` on attribute access,
    #: and ``.__set__`` when setting attribute value, and those
    #: will use the underlying ``Settings._broker_request_timeout``
    #: storage attribute to access/store the current value.
    storage_name: str

    #: Default value for setting.
    #:
    #: Note that this will not be used if :attr:`default_alias` or
    #: :attr:`defaut_template` is set.
    default: IT = cast(IT, None)

    #: Environment variable name for setting.
    #: For :setting:`broker_request_timeout` this would be
    #: ``env_name="BROKER_REQUEST_TIMEOUT"``
    env_name: Optional[str] = None

    #: If setting is not customized this is an optional
    #: list of other settings that we should take default value from.
    #: For example the :setting:`broker_consumer`/:setting:`broker_producer`
    #: settings can configure the broker URL for consumers and producers
    #: separately but take their default value from the `broker` setting.
    default_alias: Optional[str] = None

    #: Default template.
    #: If set the default value will be generated from this format string
    #: template.
    #: For exmaple the :setting:`canonical_url` setting uses
    #: ``default_template='http://{conf.web_host}:{conf.web_port}' to
    #: generate a default value from the :setting:`web_host` and
    #: :setting:`web_port` settings.
    default_template: Optional[str] = None

    #: Set to true if the value can be :const:`None`.
    allow_none: bool = False

    # If set to True we don't modify the value
    # of the attribute to set a default.
    # This is used by e.g. the env_prefix setting
    # which has custom constructor code in Settings.on_init
    ignore_default: bool = False

    #: The configuration section this setting belongs to.
    #: E.g. ``sections.Common``.
    section: _Section

    #: The version that this setting was first introduced.
    #: This is used by :file:`extra/tools/render_configuration_reference.py`
    #: to generate a version added directive:
    #:
    #: .. sourcecode:: restructuredtext
    #:
    #:    .. versionadded:: 1.10
    version_introduced: Optional[str] = None

    #: Set this if the setting is deprecated and should not be used anymore.
    #: Deprecated settings are not added to the configuration reference.
    #: Note: You must also set a :attr:`deprecation_reason`.
    version_deprecated: Optional[str] = None
    deprecation_reason: Optional[str] = None

    #: Mapping of version changes and reason for changing.
    #: This is used by :file:`extra/tools/render_configuration_reference.py`
    #: For example if this was enabled by default but then changed
    #: to be disabled by default in version 1.30, then you can specify
    #: that as ``version_changed={'1.30': 'Disabled by default.'}``
    #: and the configuration reference will be rendered with the
    #: following version changed directive added:
    #:
    #: .. sourcecode:: restructuredtext
    #:
    #:    .. versionchanged:: 1.30
    #:
    #:         Disabled by default.
    version_changed: Optional[Mapping[str, str]] = None

    #: Set this if the setting should be disabled completely,
    #: but still be included in the code.
    #: This is rare, no setting should be included in the
    #: code if it has been removed.  Currently this is only
    #: used for the example setting that describes how you can add new
    #: settings.
    version_removed: Optional[str] = None

    #: Mapping of related command line options.
    #: This should be a mapping from command name to a list of option names.
    #:
    #: For example the :setting:`canonical_url` setting lists related
    #: options as:
    #:
    #: .. sourcecode:: python
    #:
    #:    related_cli_options={
    #:      'faust worker': ['--web-host', '--web-port'],
    #:    }
    #:
    #: And this will end up in the configuration reference as:
    #:
    #: .. sourcecode:: restructuredtext
    #:
    #:   :related-options: :option:`faust worker --web-host`,
    #:                     :option:`faust worker --web-port`
    related_cli_options: Mapping[str, List[str]]

    #: List of related settings.
    #: For example for the :setting:`canonical_url` setting
    #: the list of related settings are defined as:
    #: ``related_settings=[web_host, web_port]``.
    #: The configuration reference will then include that as:
    #:
    #: .. sourcecode:: restructuredtext
    #:
    #:    :related-settings: :setting:`web_host`, :setting:`web_port`
    related_settings: List[Any]

    #: Template used to generate a deprecation warning for deprecated settings.
    deprecation_warning_template: str = DEPRECATION_WARNING_TEMPLATE

    #: Template used to generate an additional removal warning
    #: for the deprecation warning.
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
                 related_settings: List[Any] = None,
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
        """Use in subclasses to quickly override ``__init__``."""
        ...

    def on_get_value(self, fun: OutputCallable) -> OutputCallable:
        """Decorator that adds a callback when this setting is retrieved."""
        assert self._on_get_value_ is None
        self._on_get_value_ = fun
        return fun

    def on_set_default(self, fun: OnDefaultCallable) -> OnDefaultCallable:
        """Decorator that adds a callback when a default value is used."""
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
        """What happens when the setting is accessed/retrieved."""
        value = getattr(conf, self.storage_name)
        if value is None and self.default_alias:
            retval = getattr(conf, self.default_alias)
        else:
            retval = self.prepare_get(conf, value)
        if self._on_get_value_ is not None:
            return self._on_get_value_(conf, retval)
        return retval

    def prepare_get(self, conf: _Settings, value: OT) -> OT:
        """Prepare value when accessed/retrieved."""
        return value

    def on_set(self, settings: Any, value: OT) -> None:
        """What happens when the setting is stored/set."""
        settings.__dict__[self.storage_name] = value
        assert getattr(settings, self.storage_name) == value

    def set_class_default(self, cls: Type) -> None:
        """Set class default value for storage attribute."""
        setattr(cls, self.storage_name, self.default)

    def on_init_set_value(self,
                          conf: _Settings,
                          provided_value: Optional[IT]) -> None:
        """What happens at ``Settings.__init__`` to store provided value.

        Arguments:
            conf: Settings object.
            provided_value: Provided configuration value passed to
                            ``Settings.__init__`` or :const:`None` if not set.
        """
        if provided_value is not None:
            self.__set__(conf, provided_value)

    def on_init_set_default(self,
                            conf: _Settings,
                            provided_value: Optional[IT]) -> None:
        """What happens at ``Settings.__init__`` to set default value.

        Arguments:
            conf: Settings object.
            provided_value: Provided configuration value passed to
                            ``Settings.__init__`` or :const:`None` if not set.
        """
        if provided_value is None:
            default_value = self.default
            if self._on_set_default_:
                default_value = self._on_set_default_(conf)
            if default_value is None and self.default_template:
                default_value = self.default_template.format(conf=conf)
            setattr(conf, self.storage_name,
                    self.prepare_init_default(conf, default_value))

    def build_deprecation_warning(self) -> str:
        """Build deprecation warning for this setting."""
        alt_removal = ''
        if self.version_removed:
            alt_removal = self.deprecation_removal_warning.format(self=self)
        return self.deprecation_warning_template.format(
            self=self,
            alt_removal=alt_removal,
        )

    def validate_before(self, value: IT = None) -> None:
        """Validate value before setting is converted to the target type."""
        ...

    def validate_after(self, value: OT) -> None:
        """Validate value after it has been converted to its target type."""
        ...

    def prepare_set(self, conf: _Settings, value: IT) -> OT:
        """Prepare value for storage."""
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
        """Prepare default value for storage."""
        if value is not None:
            return self.to_python(conf, value)
        return None

    def to_python(self, conf: _Settings, value: IT) -> OT:
        """Convert value in input type to its output type."""
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
    """Boolean setting type."""
    text_type = (bool,)

    def to_python(self, conf: _Settings, value: Any) -> bool:
        """Convert given value to :class:`bool`."""
        if isinstance(value, str):
            return to_bool(value)
        return bool(value)


class Str(Param[str, str]):
    """String setting type."""
    text_type = (str,)


class Severity(Param[_Severity, _Severity]):
    """Logging severity setting type."""
    text_type = (str, int)


class Number(Param[IT, OT]):
    """Number setting type (baseclass for int/float)."""
    min_value: Optional[int] = None
    max_value: Optional[int] = None
    number_aliases: Mapping[IT, OT]

    def _init_options(self,
                      min_value: int = None,
                      max_value: int = None,
                      number_aliases: Mapping[IT, OT] = None,
                      **kwargs: Any) -> None:
        if min_value is not None:
            self.min_value = min_value
        if max_value is not None:
            self.max_value = max_value
        self.number_aliases = number_aliases or {}

    @abc.abstractmethod
    def convert(self, conf: _Settings, value: IT) -> OT:
        ...

    def to_python(self,
                  conf: _Settings,
                  value: IT) -> OT:
        """Convert given value to number."""
        try:
            return self.number_aliases[value]
        except KeyError:
            return self.convert(conf, value)

    def validate_after(self, value: OT) -> None:
        """Validate number value."""
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
    text_type = (int,)

    def convert(self,
                conf: _Settings,
                value: IT) -> OT:
        """Convert given value to int."""
        return cast(OT, int(cast(int, value)))


class Int(_Int[NumberInputArg, int]):
    """Signed integer setting type."""


class UnsignedInt(_Int[NumberInputArg, int]):
    """Unsigned integer setting type."""
    min_value = 0


class Version(Int):
    """Version setting type.

    Versions must be greater than ``1``.
    """
    min_value = 1


class Port(UnsignedInt):
    """Network port setting type.

    Ports must be in the range 1-65535.
    """
    min_value = 1
    max_value = 65535


class Seconds(Param[_Seconds, float]):
    """Seconds setting type.

    Converts from :class:`float`/:class:`~datetime.timedelta` to
    :class:`float`.
    """
    text_type = (float, timedelta)

    def to_python(self, conf: _Settings, value: _Seconds) -> float:
        return want_seconds(value)


class Credentials(Param[CredentialsArg, Optional[CredentialsT]]):
    """Authentication credentials setting type."""
    text_type = (CredentialsT,)

    def to_python(self,
                  conf: _Settings,
                  value: CredentialsArg) -> Optional[CredentialsT]:
        return to_credentials(value)


class SSLContext(Param[ssl.SSLContext, Optional[ssl.SSLContext]]):
    """SSL context setting type."""
    text_type = (ssl.SSLContext,)


class Dict(Param[DictArg[T], Mapping[str, T]]):
    """Dictionary setting type."""
    text_type = (dict,)

    def to_python(self,
                  conf: _Settings,
                  value: DictArg[T]) -> Mapping[str, T]:
        if isinstance(value, str):
            return json.loads(value)
        elif isinstance(value, Mapping):
            return value
        return dict(value)


class LogHandlers(Param[List[logging.Handler], List[logging.Handler]]):
    """Log handler list setting type."""
    text_type = (List[logging.Handler],)

    def prepare_init_default(
            self, conf: _Settings, value: Any) -> List[logging.Handler]:
        return []


class Timezone(Param[Union[str, tzinfo], tzinfo]):
    """Timezone setting type."""
    text_type = (tzinfo,)
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
    """Broker URL list setting type."""
    text_type = (str, _URL, List[str])
    default_scheme = DEFAULT_BROKER_SCHEME

    def to_python(self, conf: _Settings, value: BrokerArg) -> List[_URL]:
        return self.broker_list(value)

    def broker_list(self, value: BrokerArg) -> List[_URL]:
        return urllist(value, default_scheme=self.default_scheme)


class URL(Param[URLArg, _URL]):
    """URL setting type."""
    text_type = (str, _URL)

    def to_python(self, conf: _Settings, value: URLArg) -> _URL:
        return _URL(value)


class Path(Param[Union[str, _Path], _Path]):
    """Path setting type."""
    text_type = (str, _Path)
    expanduser: bool = True

    def to_python(self, conf: _Settings, value: Union[str, _Path]) -> _Path:
        p = _Path(value)
        if self.expanduser:
            p = p.expanduser()
        return self.prepare_path(conf, p)

    def prepare_path(self, conf: _Settings, path: _Path) -> _Path:
        return path


class Codec(Param[CodecArg, CodecArg]):
    """Serialization codec setting type."""
    text_type = (str, CodecT)


def Enum(typ: T) -> Type[Param[Union[str, T], T]]:
    """Generate new enum setting type."""

    class EnumParam(Param[Union[str, T], T]):
        text_type = (str,)

        def to_python(self, conf: _Settings, value: Union[str, T]) -> T:
            return typ(value)  # type: ignore

    return EnumParam


class _Symbol(Param[IT, OT]):
    text_type = (str, Type)

    def to_python(self, conf: _Settings, value: IT) -> OT:
        return cast(OT, symbol_by_name(value))


def Symbol(typ: T) -> Type[Param[SymbolArg[T], T]]:
    """Generate new symbol setting type."""
    return _Symbol[SymbolArg[T], T]
