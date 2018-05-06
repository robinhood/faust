"""Utilities for generating code at runtime."""
from typing import Any, Callable, Dict, List

__all__ = ['Function', 'Method', 'InitMethod']

MISSING = object()


def Function(name: str,
             args: List[str],
             body: List[str],
             *,
             globals: Dict[str, Any] = None,
             locals: Dict[str, Any] = None,
             return_type: Any = MISSING,
             argsep: str = ', ') -> Callable:
    assert locals is not None
    return_annotation = ''
    if return_type is not MISSING:
        locals['_return_type'] = return_type
        return_annotation = '->_return_type'
    bodys = '\n'.join(f'  {b}' for b in body)

    src = f'def {name}({argsep.join(args)}){return_annotation}:\n{bodys}'
    exec(src, globals, locals)
    obj = locals[name]
    obj.__sourcecode__ = src
    return obj


def Method(name: str,
           args: List[str],
           body: List[str],
           **kwargs: Any) -> Callable:
    return Function(name, ['self'] + args, body, **kwargs)


def InitMethod(args: List[str],
               body: List[str],
               **kwargs: Any) -> Callable[[], None]:
    return Method('__init__', args, body, return_type='None', **kwargs)
