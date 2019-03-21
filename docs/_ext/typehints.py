import inspect
from typing import Any, AnyStr, TypeVar, get_type_hints
from sphinx.util.inspect import Signature

try:
    from typing import GenericMeta  # Py3.7
except ImportError:
    from typing import Generic as GenericMeta  # Py3.7


def format_annotation(annotation):
    if inspect.isclass(annotation) and annotation.__module__ == 'builtins':
        if annotation.__qualname__ == 'NoneType':
            return '``None``'
        else:
            return ':py:class:`{}`'.format(annotation.__qualname__)

    annotation_cls = (
        annotation if inspect.isclass(annotation) else type(annotation))
    if annotation_cls.__module__ == 'typing':
        params = None
        prefix = ':py:class:'
        extra = ''
        class_name = annotation_cls.__qualname__
        if annotation is Any:
            return ':py:data:`~typing.Any`'
        elif annotation is AnyStr:
            return ':py:data:`~typing.AnyStr`'
        elif isinstance(annotation, TypeVar):
            return f'\\{annotation!r}'
        elif class_name in ('Union', '_Union'):
            prefix = ':py:data:'
            class_name = 'Union'
            if hasattr(annotation, '__union_params__'):
                params = annotation.__union_params__
            else:
                params = annotation.__args__

            if params and len(params) == 2 and (
                    hasattr(params[1], '__qualname__') and
                    params[1].__qualname__ == 'NoneType'):
                class_name = 'Optional'
                params = (params[0],)
        elif annotation_cls.__qualname__ == 'Tuple' and (
                hasattr(annotation, '__tuple_params__')):
            params = annotation.__tuple_params__
            if annotation.__tuple_use_ellipsis__:
                params += (Ellipsis,)
        elif annotation_cls.__qualname__ == 'Callable':
            prefix = ':py:data:'
            arg_annotations = result_annotation = None
            if hasattr(annotation, '__result__'):
                arg_annotations = annotation.__args__
                result_annotation = annotation.__result__
            elif getattr(annotation, '__args__', None) is not None:
                arg_annotations = annotation.__args__[:-1]
                result_annotation = annotation.__args__[-1]

            if arg_annotations in (Ellipsis, (Ellipsis,)):
                params = [Ellipsis, result_annotation]
            elif arg_annotations is not None:
                params = [
                    '\\[{}]'.format(
                        ', '.join(format_annotation(param)
                                  for param in arg_annotations)),
                    result_annotation,
                ]
        elif hasattr(annotation, 'type_var'):
            # Type alias
            class_name = annotation.name
            params = (annotation.type_var,)
        elif getattr(annotation, '__args__', None) is not None:
            params = annotation.__args__
        elif hasattr(annotation, '__parameters__'):
            params = annotation.__parameters__

        if params:
            extra = '\\[{}]'.format(
                ', '.join(format_annotation(param) for param in params))

        return '{}`~typing.{}`{}'.format(prefix, class_name, extra)
    elif annotation is Ellipsis:
        return '...'
    elif inspect.isclass(annotation):
        extra = ''
        if isinstance(annotation, GenericMeta):
            extra = '\\[{}]'.format(', '.join(
                format_annotation(param)
                for param in annotation.__parameters__))

        return ':py:class:`~{}.{}`{}'.format(
            annotation.__module__,
            annotation.__qualname__,
            extra)
    else:
        return str(annotation)


def process_signature(app, what: str, name: str, obj, options,
                      signature, return_annotation):
    if callable(obj) and getattr(obj, '__annotations__', None):
        if what in ('class', 'exception'):
            obj = getattr(obj, '__init__')

        bound_method = what in ('method', 'class', 'exception')
        obj = inspect.unwrap(obj)
        try:
            sig = Signature(obj, bound_method=bound_method)
        except (TypeError, ValueError):
            return

        formatted_args = sig.format_args()
        return formatted_args, None


def process_docstring(app, what, name, obj, options, lines):
    if isinstance(obj, property):
        obj = obj.fget

    if callable(obj):
        if what in ('class', 'exception'):
            obj = getattr(obj, '__init__')

        obj = inspect.unwrap(obj)
        try:
            type_hints = get_type_hints(obj)
        except (AttributeError, TypeError):
            # Introspecting a slot wrapper will raise TypeError
            return

        for argname, annotation in type_hints.items():
            formatted_annotation = format_annotation(annotation)

            if argname == 'return':
                if what in ('class', 'exception'):
                    # Don't add return type None from __init__()
                    continue

                insert_index = len(lines)
                for i, line in enumerate(lines):
                    if line.startswith(':rtype:'):
                        insert_index = None
                        break
                    elif line.startswith((':return:', ':returns:')):
                        insert_index = i
                        break

                if insert_index is not None:
                    lines.insert(insert_index, ':rtype: {}'.format(
                        formatted_annotation))
            else:
                searchfor = ':param {}:'.format(argname)
                for i, line in enumerate(lines):
                    if line.startswith(searchfor):
                        lines.insert(i, ':type {}: {}'.format(
                            argname, formatted_annotation))
                        break
                searchfor2 = ':keyword {}:'.format(argname)
                for i, line in enumerate(lines):
                    line.replace(':keyword ', ':param ')
                    if line.startswith(searchfor2):
                        lines.insert(i, ':type {}: {}'.format(
                            argname, formatted_annotation))
                        break


def setup(app):
    app.connect('autodoc-process-signature', process_signature)
    app.connect('autodoc-process-docstring', process_docstring)
    return {'parallel_read_safe': True}
