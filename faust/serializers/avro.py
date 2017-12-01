"""Apache Avro serialization support."""
from typing import Any, Dict, List, Mapping, Sequence, Tuple, Type
from ..types import ModelT

__all__ = ['to_avro_type']

AVRO_FAST_TYPE: Mapping[Any, str] = {
    int: 'int',
    float: 'float',
    bool: 'boolean',
    str: 'string',
    list: 'array',
    List: 'array',
    Sequence: 'array',
    Tuple: 'array',
    Mapping: 'map',
    Dict: 'map',
    dict: 'map',
}


def to_avro_type(typ: Type) -> str:
    """Convert Python type to Avro type name."""
    if typ in AVRO_FAST_TYPE:
        return AVRO_FAST_TYPE[typ]
    elif issubclass(typ, Sequence):
        return 'array'
    elif issubclass(typ, Mapping):
        return 'map'
    elif issubclass(typ, ModelT):
        return typ.as_schema()
    raise TypeError(f'Cannot convert type {typ!r} to Avro')
