import abc
import typing
import warnings
from collections import defaultdict
from typing import Any, ClassVar, Dict, List, Set, cast
from faust.exceptions import AlreadyConfiguredWarning
from .params import Param

if typing.TYPE_CHECKING:
    from .sections import Section as _Section
    from .settings import Settings as _Settings
else:
    class _Section: ...   # noqa: E701
    class _Settings: ...  # noqa: E701

W_ALREADY_CONFIGURED = '''\
App is already configured.

Reconfiguring late may mean parts of your program are still
using the old configuration.

Code such as:

app.conf.config_from_object('my.config')

Should appear before calling app.topic/@app.agent/etc.
'''

W_ALREADY_CONFIGURED_KEY = '''\
Setting new value for configuration key {key!r} that was already used.

Reconfiguring late may mean parts of your program are still
using the old value of {old_value!r}.

Code such as:

app.conf.{key} = {value!r}

Should appear before calling app.topic/@app.agent/etc.
'''

SettingIndexMapping = Dict[str, Param]
SettingSectionIndexMapping = Dict[_Section, List[Param]]


class SettingsRegistry(abc.ABC):
    """Base class used for :class:`faust.types.settings.Settings`."""

    #: Index of all settings by name.
    SETTINGS: ClassVar[SettingIndexMapping] = {}

    #: Index of all sections and the settings in a section.
    SETTINGS_BY_SECTION: ClassVar[SettingSectionIndexMapping] = {}

    _initializing: bool = True
    _accessed: Set[str] = cast(Set[str], None)

    @classmethod
    def setting_names(cls) -> Set[str]:
        """Return set of all active setting names."""
        return {
            name for name, setting in cls.SETTINGS.items()
            if setting.active and not setting.deprecated
        }

    @classmethod
    def _init_subclass_settings(cls) -> None:
        settings: SettingIndexMapping = dict(cls.SETTINGS)
        by_section: SettingSectionIndexMapping = defaultdict(list)
        by_section.update(cls.SETTINGS_BY_SECTION)

        # Find and and index all Param attributes
        for name, attr in vars(cls).items():
            try:
                is_param = isinstance(attr, Param)
            except TypeError:
                pass
            else:
                if is_param and attr.active:
                    settings[name] = attr
                    by_section[attr.section].append(attr)
                    setattr(cls, name, attr)
        cls.SETTINGS = settings
        cls.SETTINGS_BY_SECTION = by_section

        # Settings.__init__ defines all input parameters
        # so typing is intact, but we want to use the
        # parameters as keyword arguments so we just
        # replace Settings.__init__ with a new init method
        # that puts the arguments into kwargs :-)
        def _new_init(self: _Settings, *args: Any, **kwargs: Any) -> None:
            self._init_entrypoint(*args, **kwargs)
        cls.__init__ = _new_init  # type: ignore

        # For every setting there will be an accessor property
        # and a storage value.
        # E.g. for the setting broker_request_timeout,
        # Settings.broker_request_timeout is the accessor property
        # and Settings._broker_request_timeout is the storage
        # where the current value is stored and that the accessor
        # property reads from.
        for param in cls.SETTINGS.values():
            param.set_class_default(cls)

    @classmethod
    def _warn_already_configured(cls) -> None:
        warnings.warn(AlreadyConfiguredWarning(W_ALREADY_CONFIGURED),
                      stacklevel=3)

    @classmethod
    def _warn_already_configured_key(cls,
                                     key: str,
                                     value: Any,
                                     old_value: Any) -> None:
        warnings.warn(
            AlreadyConfiguredWarning(W_ALREADY_CONFIGURED_KEY.format(
                key=key,
                value=value,
                old_value=old_value,
            )),
            stacklevel=3,
        )

    def __init_subclass__(self) -> None:
        self._init_subclass_settings()

    @abc.abstractmethod
    def on_init(self, id: str, **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def getenv(self, env_name: str) -> Any:
        ...

    def _init_entrypoint(self, *args: Any, **kwargs: Any) -> None:
        self._on_before_init()
        self.on_init(*args, **kwargs)
        self._init_settings(kwargs)
        self._on_after_init()
        self.__post_init__()

    def _on_before_init(self) -> None:
        # this set keeps track of settings that have been read
        # we do not want to do that when setting attributes so
        # we set it to None.
        # After init has set all attributes on_after_init will enable
        # read tracking by initializing this to an empty set.
        object.__setattr__(self, '_accessed', None)

    def _init_settings(self, kwargs: Dict) -> None:
        # called by Settings.__init__(**kwargs) to configure
        # available settings.
        conf = cast(_Settings, self)
        for name, param in self.SETTINGS.items():
            if not param.ignore_default:
                provided_value = kwargs.get(name, None)
                if param.env_name is not None:
                    provided_env_value = self.getenv(param.env_name)
                    if provided_env_value is not None:
                        provided_value = kwargs[name] = provided_env_value
                param.on_init_set_value(conf, provided_value)
        for name, param in self.SETTINGS.items():
            if not param.ignore_default:
                provided_value = kwargs.get(name, None)
                param.on_init_set_default(conf, provided_value)

    def _on_after_init(self) -> None:
        object.__setattr__(self, '_accessed', set())
        object.__setattr__(self, '_initializing', False)

    def __post_init__(self) -> None:
        ...

    def __getattribute__(self, key: str) -> Any:
        accessed = object.__getattribute__(self, '_accessed')
        if not key.startswith('_'):
            if not object.__getattribute__(self, '_initializing'):
                accessed.add(key)
        return object.__getattribute__(self, key)

    def __setattr__(self, key: str, value: Any) -> None:
        xsd = object.__getattribute__(self, '_accessed')
        if xsd is not None and not key.startswith('_') and key in xsd:
            old_value = object.__getattribute__(self, key)
            self._warn_already_configured_key(key, value, old_value)
        object.__setattr__(self, key, value)
