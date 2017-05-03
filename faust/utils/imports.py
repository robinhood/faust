"""Importing utilities."""
import importlib
import sys
import warnings
from typing import Any, Iterable, Mapping, MutableMapping, Tuple, Type, Union
from .collections import FastUserDict
from .objects import cached_property

# - these are taken from kombu.utils.imports

__all__ = [
    'FactoryMapping', 'SymbolArg', 'qualname', 'symbol_by_name',
    'load_extension_class_names', 'load_extension_classes',
]


SymbolArg = Union[str, Type]


class FactoryMapping(FastUserDict):
    """Class plugin system.

    This is an utility to maintain a mapping from name to fully
    qualified Python attribute path, and also supporting the use
    of these in URLs.

    Example:

        >>> drivers = FactoryMapping({
        ...    'rabbitmq': 'my.drivers.rabbitmq:Driver',
        ...    'kafka': 'my.drivers.kafka:Driver',
        ...    'redis': 'my.drivers.redis:Driver',
        ... })

        >>> drivers.by_url('rabbitmq://localhost:9090')
        <class 'my.drivers.rabbitmq.Driver'>

        >>> drivers.by_name('redis')
        <class 'my.drivers.redis.Driver'>
    """

    aliases: MutableMapping[str, str]

    def __init__(self, *args: Mapping, **kwargs: str) -> None:
        self.aliases = dict(*args, **kwargs)  # type: ignore

    def by_url(self, url: str) -> Type:
        """Get class associated with URL (scheme is used as alias key)."""
        # we remove anything after ; so urlparse can recognize the url.
        return self.by_name(url.partition('://')[0])

    def by_name(self, name: SymbolArg) -> Any:
        return symbol_by_name(name, aliases=self.aliases)

    @cached_property
    def data(self) -> MutableMapping:  # type: ignore
        return self.aliases


def qualname(obj: Any) -> str:
    """Get object qualified name."""
    if not hasattr(obj, '__name__') and hasattr(obj, '__class__'):
        obj = obj.__class__
    return '.'.join((obj.__module__, obj.__name__))


def symbol_by_name(
        name: SymbolArg,
        aliases: Mapping[str, str] = {},
        imp: Any = None,
        package: str = None,
        sep: str = '.',
        default: Any = None,
        **kwargs: Any) -> Any:
    """Get symbol by qualified name.

    The name should be the full dot-separated path to the class::

        modulename.ClassName

    Example::

        mazecache.backends.redis.RedisBackend
                                ^- class name

    or using ':' to separate module and symbol::

        mazecache.backends.redis:RedisBackend

    If `aliases` is provided, a dict containing short name/long name
    mappings, the name is looked up in the aliases first.

    Examples:

        >>> symbol_by_name('mazecache.backends.redis:RedisBackend')
        <class 'mazecache.backends.redis.RedisBackend'>

        >>> symbol_by_name('default', {
        ...     'default': 'mazecache.backends.redis:RedisBackend'})
        <class 'mazecache.backends.redis.RedisBackend'>

        # Does not try to look up non-string names.
        >>> from mazecache.backends.redis import RedisBackend
        >>> symbol_by_name(RedisBackend) is RedisBackend
        True

    """
    # This code was copied from kombu.utils.symbol_by_name
    if imp is None:
        imp = importlib.import_module

    if not isinstance(name, str):
        return name                                 # already a class

    name = aliases.get(name) or name
    sep = ':' if ':' in name else sep
    module_name, _, cls_name = name.rpartition(sep)
    if not module_name:
        cls_name, module_name = None, package if package else cls_name
    try:
        try:
            module = imp(module_name, package=package, **kwargs)
        except ValueError as exc:
            raise ValueError(
                "Couldn't import {0!r}: {1}".format(name, exc)
            ).with_traceback(sys.exc_info()[2])
        return getattr(module, cls_name) if cls_name else module
    except (ImportError, AttributeError):
        if default is None:
            raise
    return default


def load_extension_class_names(namespace: str) -> Iterable[Tuple[str, str]]:
    try:
        from pkg_resources import iter_entry_points
    except ImportError:  # pragma: no cover
        return

    for ep in iter_entry_points(namespace):
        yield ep.name, ':'.join([ep.module_name, ep.attrs[0]])


def load_extension_classes(namespace: str) -> Iterable[Tuple[str, Type]]:
    for name, class_name in load_extension_class_names(namespace):
        try:
            cls = symbol_by_name(class_name)
        except (ImportError, SyntaxError) as exc:
            warnings.warn(
                'Cannot load {0} extension {1!r}: {2!r}'.format(
                    namespace, class_name, exc))
        else:
            yield name, cls
