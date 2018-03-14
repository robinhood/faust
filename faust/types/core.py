import typing
from typing import Any, Optional, Union

if typing.TYPE_CHECKING:
    from .models import ModelT
else:
    class ModelT: ...  # noqa

__all__ = ['K', 'V']

#: Shorthand for the type of a key
K = Optional[Union[bytes, ModelT, Any]]

#: Shorthand for the type of a value
V = Union[bytes, ModelT, Any]
