"""Serialization utilities.

Supported serializers
=====================

* **json**    - json with utf-8 encoding.
* **pickle**  - pickle with base64 encoding (not urlsafe)
* **binary**  - base64 encoding (not urlsafe)
* **text**    - text encoding, utf-16.

Serialization by name
=====================

The func:`dumps` function takes a serializer name and the object to serialize:

.. code-block:: pycon

    >>> s = dumps('json', obj)

For the reverse direction, the func:`loads` function takes a serializer
name and a serialized payload:

.. code-block:: pycon

    >>> obj = loads('json', s)

You can also combine encoders in the name, like in this case
where json is combined with gzip compression:

.. code-block:: pycon

    >>> obj = loads('json|gzip', s)

Serializer registry
===================

Serializers are configured by name and this module maintains
a mapping from name to :class:`Serializer` instance: the :attr:`serializers`
attribute.

You can add a new serializer to this mapping by:

.. code-block:: pycon

    >>> from faust.utils import serialization
    >>> serialization.serializers['custom'] = custom_serializer()

A serializer subclass requires two methods to be implemented: ``_loads()``
and ``_dumps()``:

.. code-block:: python

    import msgpack

    from faust.utils.serialization import Serializer, binary, serializers

    class raw_msgpack(Serializer):

        def _dumps(self, obj):
            return msgpack.dumps(obj)

        def _loads(self, s):
            return msgpack.loads(s)

Our serializer now encodes/decodes to raw msgpack format, but we
may also need to transfer this payload on a transport not
handling binary data well.  Serializers can be chained together,
so to add a text encoding like base64, which we use in this case,
we use the ``|`` operator to form a combined serializer:

.. code-block:: python

    def msgpack():
        return raw_msgpack() | binary()

    serializers['msgpack'] = msgpack()

At this point we monkey-patched Faust to support
our serializer, and we can use it to define event types:

.. code-block:: pycon

    >>> from faust import Event
    >>> class Point(Event, serializer='msgpack'):
    ...     x: int
    ...     y: int

The problem with monkey-patching is that we must make sure the patching
happens before we use the feature.

Faust also supports registering *serializer extensions*
using setuptools entrypoints, so instead we can create an installable msgpack
extension.

To do so we need to define a package with the following directory layout:

.. code-block:: text

    faust-msgpack/
        setup.py
        faust_msgpack.py

The first file, :file:`faust-msgpack/setup.py`, defines metadata about our
package and should look like the following example:

.. code-block:: python

    import setuptools

    setuptools.setup(
        name='faust-msgpack',
        version='1.0.0',
        description='Faust msgpack serialization support',
        author='Ola A. Normann',
        author_email='ola@normann.no',
        url='http://github.com/example/faust-msgpack',
        platforms=['any'],
        license='BSD',
        packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
        zip_safe=False,
        install_requires=['msgpack-python'],
        tests_require=[],
        entry_points={
            'faust.serializers': [
                'msgpack = faust_msgpack:msgpack',
            ],
        },
    )

The most important part being the ``entry_points`` key which tells
Faust how to load our plugin. We have set the name of our
serializer to ``msgpack`` and the path to the serializer class
to be ``faust_msgpack:msgpack``. This will be imported by Faust
as ``from faust_msgpack import msgpack``, so we need to define
that part next in our :file:`faust-msgpack/faust_msgpack.py` module:

.. code-block:: python

    from __future__ import absolute_import, unicode_literals

    from faust.utils.serialization import Serializer, binary

    class raw_msgpack(Serializer):

        def _dumps(self, obj):
            return msgpack.dumps(s)


    def msgpack():
        return raw_msgpack() | binary()

That's it! To install and use our new extension we do:

.. code-block:: console

    $ python setup.py install

At this point may want to publish this on PyPI to share
the extension with other Faust users.
"""
from base64 import b64encode, b64decode
from functools import reduce
from typing import (
    Any, Dict, MutableMapping, Optional, Union, Tuple, cast,
)
from . import json as _json
from .imports import load_extension_classes
try:
    import cPickle as _pickle
except ImportError:  # pragma: no cover
    import pickle as _pickle  # type: ignore

#: Argument to loads/dumps can be str or Serializer instance.
SerializerArg = Union['Serializer', str]


class Serializer:
    """Base class for serializers."""

    #: children contains the serializers below us.
    children: Tuple['Serializer', ...]

    #: cached version of children with this serializer first.
    # could use chain below, but seems premature so just copying the list.
    nodes: Tuple['Serializer', ...]

    #: subclasses can support keyword arguments,
    #: the base implementation of :meth:`clone` uses this to
    #: preserve keyword arguments in copies.
    kwargs: Dict

    def __init__(self,
                 children: Tuple['Serializer', ...] = None,
                 **kwargs) -> None:
        self.children = children or ()
        self.nodes = (self,) + self.children
        self.kwargs = kwargs

    def _loads(self, s: Any) -> Any:
        # subclasses must implement this method.
        raise NotImplementedError()

    def _dumps(self, s: Any) -> Any:
        # subclasses must implement this method.
        return NotImplementedError()

    def dumps(self, obj: Any) -> Any:
        """Serialize object ``obj``."""
        # send _dumps to this instance, and all children.
        return reduce(lambda obj, e: e._dumps(obj), self.nodes, obj)

    def loads(self, s: Any) -> Any:
        """Deserialize object from string."""
        # send _loads to this instance, and all children in reverse order
        return reduce(lambda s, d: d._loads(s), reversed(self.nodes), s)

    def clone(self, *children: 'Serializer') -> 'Serializer':
        """Create a clone of this serializer, with optional children added."""
        return type(self)(children=self.children + children, **self.kwargs)

    def __or__(self, other: Any) -> Any:
        # serializers can be chained together, e.g. binary() | json()
        if isinstance(other, Serializer):
            return self.clone(other)
        return NotImplemented

    def __repr__(self) -> str:
        return ' | '.join(
            '{0}({1})'.format(type(n).__name__,
                              ', '.join(map(repr, n.kwargs.values())))
            for n in self.nodes
        )


class json(Serializer):
    """:mod:`json` serializer."""

    def _loads(self, s: Any) -> Any:
        return _json.loads(s)

    def _dumps(self, s: Any) -> Any:
        return _json.dumps(s)


class raw_pickle(Serializer):
    """:mod:`pickle` serializer with no encoding."""

    def _loads(self, s: Any) -> Any:
        return _pickle.loads(s)

    def _dumps(self, obj: Any) -> Any:
        return _pickle.dumps(obj)


def pickle() -> Serializer:
    """:mod:`pickle` serializer with base64 encoding."""
    return raw_pickle() | binary()


class binary(Serializer):
    """Serializer for binary content (uses Base64 encoding)."""

    def _loads(self, s: Any) -> Any:
        return b64decode(s)

    def _dumps(self, s: Any) -> Any:
        return b64encode(s)


class text_encoding(Serializer):
    """Encode/decode text."""

    def __init__(self, encoding: str, **kwargs) -> None:
        self.encoding = encoding
        super(text_encoding, self).__init__(encoding=encoding, **kwargs)

    def _loads(self, s: Any) -> Any:
        return s.decode(self.encoding)

    def _dumps(self, s: Any) -> Any:
        return s.encode(self.encoding)


def text(encoding: str = 'utf-16') -> Serializer:
    """Encode text to specific encoding."""
    return text_encoding(encoding=encoding) | binary()


#: Serializer registry, mapping of name to :class:`Serializer` instance.
serializers: MutableMapping[str, Serializer] = {
    'json': json(),
    'pickle': pickle(),
    'binary': binary(),
    'text': text(),
}

#: Cached extension classes.
#: We have to defer extension loading to runtime as the
#: extensions will import from this module causing a circular import.
_extensions_finalized: MutableMapping[str, bool] = {}


def _maybe_load_extension_classes(
        namespace: str = 'mazecache.serializers') -> None:
    if namespace not in _extensions_finalized:
        serializers.update({
            name: cls()
            for name, cls in load_extension_classes(namespace)
        })


def _reduce_node(a, b):
    return serializers.get(a, a) | serializers[b]  # type: ignore


def get_serializer(name_or_ser: SerializerArg) -> Serializer:
    """Get serializer by name."""
    _maybe_load_extension_classes()
    if isinstance(name_or_ser, str):
        if '|' in name_or_ser:
            nodes = cast(str, name_or_ser).split('|')
            # simple reduce operation, OR (|) them all together:
            return cast(Serializer, reduce(_reduce_node, nodes))
        return serializers[cast(str, name_or_ser)]
    return cast(Serializer, name_or_ser)


def dumps(serializer: Optional[SerializerArg], obj: Any) -> str:
    """Serialize object into string."""
    return get_serializer(serializer).dumps(obj) if serializer else obj


def loads(serializer: Optional[SerializerArg], s: str) -> Any:
    """Deserialize from string."""
    return get_serializer(serializer).loads(s) if serializer else s
