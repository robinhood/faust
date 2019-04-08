"""LiveCheck - Utilities."""
from typing import Any
from faust.models.base import registry

__all__ = ['to_model']


def to_model(arg: Any) -> Any:
    """Convert argument to model if possible."""
    try:
        model = registry[arg['__faust']['ns']]
    except (KeyError, TypeError):
        return arg
    else:
        return model.from_data(arg)
