"""Utilities for generating code at runtime."""
from typing import Any, Callable, Dict, List

__all__ = [
    'Function',
    'Method',
    'InitMethod',
    'HashMethod',
    'CompareMethod',
    'EqMethod',
]

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


def HashMethod(attrs: List[str], **kwargs: Any) -> Callable[[], None]:
    self_tuple = obj_attrs_tuple('self', attrs)
    return Method('__hash__',
                  [],
                  [f'return hash({self_tuple})'],
                  **kwargs)


def EqMethod(fields: List[str], **kwargs: Any) -> Callable[[], None]:
    return CompareMethod(name='__eq__', op='==', fields=fields, **kwargs)


def CompareMethod(name: str,
                  op: str,
                  fields: List[str],
                  **kwargs: Any) -> Callable[[], None]:
    # Generate object comparison function (__eq__, __le__, __gt__, etc.)

    # Examples:
    #    The example:
    #
    #       CompareMethod(
    #           name='__eq__',
    #           op='==',
    #           fields=['x', 'y'],
    #       )
    #
    #   Generates a method like this:
    #
    #       def __eq__(self, other):
    #           if other.__class__ is self.__class__:
    #               return (self.x,self.y) == (other.x,other.y)
    #           return NotImplemented
    self_tuple = obj_attrs_tuple('self', fields)
    other_tuple = obj_attrs_tuple('other', fields)
    return Method(name,
                  ['other'],
                  ['if other.__class__ is self.__class__:',
                   f' return {self_tuple}{op}{other_tuple}',
                   'return NotImplemented'],
                  **kwargs)


def obj_attrs_tuple(obj_name: str, attrs: List[str]) -> str:
    # if attrs is ['x', 'y'] and obj_name is 'self',
    # returns '(self.x,self.y)'.
    if not attrs:
        return '()'
    return f'({",".join([f"{obj_name}.{f}" for f in attrs])},)'
