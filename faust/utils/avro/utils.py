"""Avro utilities."""
from typing import Any, Dict, List, Mapping, Sequence, Tuple, Type
from faust.types.models import ModelT

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
    try:
        return AVRO_FAST_TYPE[typ]
    except KeyError:
        pass
    if issubclass(typ, Sequence):
        return 'array'
    elif issubclass(typ, Mapping):
        return 'map'
    elif issubclass(typ, ModelT):
        return typ.as_schema()  # type: ignore
    raise TypeError('Cannot convert type {!r} to Avro'.format(typ))
