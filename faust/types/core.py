import typing
from typing import (
    Any,
    Iterable,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
    Union,
    cast,
)
from mode.utils.compat import want_bytes, want_str

if typing.TYPE_CHECKING:
    from .models import ModelT
else:
    class ModelT: ...  # noqa

__all__ = ['K', 'V']

#: Shorthand for the type of a key
K = Optional[Union[bytes, ModelT, Any]]

#: Shorthand for the type of a value
V = Union[bytes, ModelT, Any]


HeadersArg = Union[Iterable[Tuple[str, bytes]], Mapping[str, bytes]]


def merge_headers(target: HeadersArg,
                  source: Mapping[str, Any]) -> HeadersArg:
    # XXX may modify in-place, but always use return value.
    if target is None:
        target = []
    if source:
        source = {want_str(k): want_bytes(v) for k, v in source.items()}
        if isinstance(target, MutableMapping):
            target.update({k: v for k, v in source.items()})
        elif isinstance(target, Mapping):
            target = dict(target)
            target.update({k: v for k, v in source.items()})
        elif isinstance(target, list):
            target.extend((h for h in source.items()))
        elif isinstance(target, tuple):
            target += tuple(h for h in source.items())
        elif isinstance(target, Iterable):
            target = list(cast(Iterable[Tuple[str, bytes]], target))
            target.extend((h for h in source.items()))
    return target
