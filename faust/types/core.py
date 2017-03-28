import faust
import typing
from typing import Optional, Union

if typing.TYPE_CHECKING:
    from .models import ModelT
else:
    class ModelT: ...  # noqa

__all__ = ['K', 'V']

#: Shorthand for the type of a key
K = Optional[Union[bytes, ModelT]]


#: Shorthand for the type of a value
V = Union[bytes, ModelT]
