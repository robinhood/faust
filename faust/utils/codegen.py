"""Utilities for generating code at runtime."""
from typing import Any, Callable, Dict, List, Mapping, Tuple, cast

__all__ = [
    'Function',
    'Method',
    'InitMethod',
    'HashMethod',
    'CompareMethod',
    'EqMethod',
    'NeMethod',
    'LeMethod',
    'LtMethod',
    'GeMethod',
    'GtMethod',
    'build_function',
    'build_function_source',
    'reprkwargs',
    'reprcall',
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
    """Compile function code object from args and body."""
    return build_function(
        name=name,
        source=build_function_source(
            name=name,
            args=args,
            body=body,
            return_type=return_type,
            argsep=argsep,
        ),
        return_type=return_type,
        globals=globals,
        locals=locals,
    )


def build_closure_source(name: str,
                         args: List[str],
                         body: List[str],
                         *,
                         outer_name: str = '__outer__',
                         outer_args: List[str] = None,
                         closures: Dict[str, str],
                         return_type: Any = MISSING,
                         indentlevel: int = 0,
                         indentspaces: int = 4,
                         argsep: str = ', ') -> str:
    inner_source = build_function_source(
        name, args, body,
        return_type=return_type,
        indentlevel=indentlevel,
        indentspaces=indentspaces,
        argsep=argsep,
    )
    closure_vars = [
        f'{local_name} = {global_name}'
        for local_name, global_name in closures.items()
    ]
    outer_source = build_function_source(
        name=outer_name,
        args=outer_args or [],
        body=closure_vars + inner_source.split('\n') + [f'return {name}'],
        return_type=MISSING,
        indentlevel=indentlevel,
        indentspaces=indentspaces,
        argsep=argsep,
    )
    return outer_source


def build_closure(outer_name: str, source: str, *args: Any,
                  return_type: Any = MISSING,
                  globals: Dict[str, Any] = None,
                  locals: Dict[str, Any] = None) -> Callable:
    assert locals is not None
    if return_type is not MISSING:
        locals['_return_type'] = return_type
    exec(source, globals, locals)
    obj = locals[outer_name](*args)
    obj.__sourcecode__ = source
    return cast(Callable, obj)


def build_function(name: str, source: str,
                   *,
                   return_type: Any = MISSING,
                   globals: Dict[str, Any] = None,
                   locals: Dict[str, Any] = None) -> Callable:
    """Generate function from Python from source code string."""
    assert locals is not None
    if return_type is not MISSING:
        locals['_return_type'] = return_type
    exec(source, globals, locals)
    obj = locals[name]
    obj.__sourcecode__ = source
    return cast(Callable, obj)


def build_function_source(name: str,
                          args: List[str],
                          body: List[str],
                          *,
                          return_type: Any = MISSING,
                          indentlevel: int = 0,
                          indentspaces: int = 4,
                          argsep: str = ', ') -> str:
    """Generate function source code from args and body."""
    indent = ' ' * indentspaces
    curindent = indent * indentlevel
    nextindent = indent * (indentlevel + 1)
    return_annotation = ''
    if return_type is not MISSING:
        return_annotation = '->_return_type'
    bodys = '\n'.join(f'{nextindent}{b}' for b in body)
    return (f'{curindent}def {name}({argsep.join(args)}){return_annotation}:\n'
            f'{bodys}')


def Method(name: str,
           args: List[str],
           body: List[str],
           **kwargs: Any) -> Callable:
    """Generate Python method."""
    return Function(name, ['self'] + args, body, **kwargs)


def InitMethod(args: List[str],
               body: List[str],
               **kwargs: Any) -> Callable[[], None]:
    """Generate ``__init__`` method."""
    return Method('__init__', args, body, return_type='None', **kwargs)


def HashMethod(attrs: List[str], **kwargs: Any) -> Callable[[], None]:
    """Generate ``__hash__`` method."""
    self_tuple = obj_attrs_tuple('self', attrs)
    return Method('__hash__',
                  [],
                  [f'return hash({self_tuple})'],
                  **kwargs)


def EqMethod(fields: List[str], **kwargs: Any) -> Callable[[], None]:
    """Generate ``__eq__`` method."""
    return CompareMethod(name='__eq__', op='==', fields=fields, **kwargs)


def NeMethod(fields: List[str], **kwargs: Any) -> Callable[[], None]:
    """Generate ``__ne__`` method."""
    return CompareMethod(name='__ne__', op='!=', fields=fields, **kwargs)


def GeMethod(fields: List[str], **kwargs: Any) -> Callable[[], None]:
    """Generate ``__ge__`` method."""
    return CompareMethod(name='__ge__', op='>=', fields=fields, **kwargs)


def GtMethod(fields: List[str], **kwargs: Any) -> Callable[[], None]:
    """Generate ``__gt__`` method."""
    return CompareMethod(name='__gt__', op='>', fields=fields, **kwargs)


def LeMethod(fields: List[str], **kwargs: Any) -> Callable[[], None]:
    """Generate ``__le__`` method."""
    return CompareMethod(name='__le__', op='<=', fields=fields, **kwargs)


def LtMethod(fields: List[str], **kwargs: Any) -> Callable[[], None]:
    """Generate ``__lt__`` method."""
    return CompareMethod(name='__lt__', op='<', fields=fields, **kwargs)


def CompareMethod(name: str,
                  op: str,
                  fields: List[str],
                  **kwargs: Any) -> Callable[[], None]:
    """Generate object comparison method.

    Excellent for ``__eq__``, ``__le__``, etc.

    Examples:
        The example:

        .. sourcecode:: python

            CompareMethod(
                name='__eq__',
                op='==',
                fields=['x', 'y'],
            )

        Generates a method like this:

        .. sourcecode:: python

           def __eq__(self, other):
              if other.__class__ is self.__class__:
                   return (self.x,self.y) == (other.x,other.y)
               return NotImplemented
    """
    self_tuple = obj_attrs_tuple('self', fields)
    other_tuple = obj_attrs_tuple('other', fields)
    return Method(name,
                  ['other'],
                  ['if other.__class__ is self.__class__:',
                   f' return {self_tuple}{op}{other_tuple}',
                   'return NotImplemented'],
                  **kwargs)


def obj_attrs_tuple(obj_name: str, attrs: List[str]) -> str:
    """Draw Python tuple from list of attributes.

    If attrs is ``['x', 'y']`` and ``obj_name`` is 'self',
    returns ``(self.x,self.y)``.
    """
    if not attrs:
        return '()'
    return f'({",".join([f"{obj_name}.{f}" for f in attrs])},)'


def reprkwargs(kwargs: Mapping[str, Any], *,
               sep: str = ', ',
               fmt: str = '{0}={1}') -> str:
    return sep.join(fmt.format(k, repr(v)) for k, v in kwargs.items())


def reprcall(name: str,
             args: Tuple = (),
             kwargs: Mapping[str, Any] = {},  # noqa: B006
             *,
             sep: str = ', ') -> str:
    return '{0}({1}{2}{3})'.format(
        name, sep.join(map(repr, args or ())),
        (args and kwargs) and sep or '',
        reprkwargs(kwargs, sep=sep),
    )
