from datetime import datetime
from decimal import Decimal
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)
import pytest
from faust.models import Record
from faust.models.typing import NodeType, TypeExpression
from faust.types import ModelT


class X(Record, namespace='test.X'):
    val: int


class Y(Record, namespace='test.Y'):
    val: str


class Z(NamedTuple):
    foo: X
    bar: int
    baz: str


class TypeExpressionTest(NamedTuple):
    type_expression: Type
    serialized_data: Any
    expected_result: Any
    expected_comprehension: str
    expected_types: Dict[NodeType, Set[Type]]


def Xi(i):
    return X(i).dumps()


CASE_TUPLE_LIST_SET_X = TypeExpressionTest(
    type_expression=Tuple[X, List[Set[X]]],
    serialized_data=[
        Xi(0),
        [[Xi(1), Xi(2), Xi(3)],
         [Xi(4), Xi(5), Xi(6)],
         [Xi(7), Xi(8)]]],
    expected_result=(
        X(0),
        [
            {X(1), X(2), X(3)},
            {X(4), X(5), X(6)},
            {X(7), X(8)},
        ],
    ),
    expected_comprehension='''\
(test__X._from_data_field(a[0]), \
[{test__X._from_data_field(c) for c in b} \
for b in a[1]])''',
    expected_types={NodeType.MODEL: {X}},
)

CASE_LIST_LIST_X = TypeExpressionTest(
    type_expression=List[List[X]],
    serialized_data=[
        [Xi(1), Xi(2), Xi(3)],
        [Xi(4), Xi(5), Xi(6)],
        [Xi(7), Xi(8), Xi(9)]],
    expected_result=[
        [X(1), X(2), X(3)],
        [X(4), X(5), X(6)],
        [X(7), X(8), X(9)],
    ],
    expected_comprehension='''\
[[test__X._from_data_field(c) for c in b] for b in a]''',
    expected_types={NodeType.MODEL: {X}},
)

CASE_DICT_KEY_STR_VALUE_OPTIONAL_SET_X_COMP = '''
    {b: ({test__X._from_data_field(d) for d in c} if c is not None else None) \
for b, c in a.items()}
'''.strip()
CASE_DICT_KEY_STR_VALUE_OPTIONAL_SET_X = TypeExpressionTest(
    type_expression=Dict[str, Optional[Set[X]]],
    serialized_data={
        'foo': [Xi(1), Xi(2), Xi(3)],
        'bar': [Xi(3), Xi(4), Xi(5)],
        'baz': [Xi(7), Xi(8)],
        'xaz': None,
        'xuz': [],
    },
    expected_result={
        'foo': {X(1), X(2), X(3)},
        'bar': {X(3), X(4), X(5)},
        'baz': {X(7), X(8)},
        'xaz': None,
        'xuz': set(),
    },
    expected_comprehension=CASE_DICT_KEY_STR_VALUE_OPTIONAL_SET_X_COMP,
    expected_types={NodeType.MODEL: {X}},
)

CASE_DICT_KEY_STR_VALUE_OPTIONAL_SET_ABSMODEL_COMP = '''
    {b: ({_Model_._from_data_field(d) for d in c} if c is not None else None) \
for b, c in a.items()}
'''.strip()
CASE_DICT_KEY_STR_VALUE_OPTIONAL_SET_ABSMODEL = TypeExpressionTest(
    type_expression=Dict[str, Optional[Set[ModelT]]],
    serialized_data={
        'foo': [Xi(1), Xi(2), Xi(3)],
        'bar': [Xi(3), Xi(4), Xi(5)],
        'baz': [Xi(7), Xi(8)],
        'xaz': None,
        'xuz': [],
    },
    expected_result={
        'foo': {X(1), X(2), X(3)},
        'bar': {X(3), X(4), X(5)},
        'baz': {X(7), X(8)},
        'xaz': None,
        'xuz': set(),
    },
    expected_comprehension=CASE_DICT_KEY_STR_VALUE_OPTIONAL_SET_ABSMODEL_COMP,
    expected_types={NodeType.MODEL: {ModelT}},
)
# List[Dict[Tuple[int, X], List[Mapping[str, Optional[Set[X]]]]]
CASE_COMPLEX1_COMP = '''\
[{(c[0], c[1]): [\
{f: ({test__X._from_data_field(h) for h in g} if g is not None else None) \
for f, g in e.items()} for e in d] for c, d in b.items()} for b in a]\
'''
CASE_COMPLEX1 = TypeExpressionTest(
    type_expression=List[Dict[
        Tuple[int, Any],
        List[Mapping[str, Optional[Set[X]]]],
    ]],
    serialized_data=[
        {
            (0, 1): [
                {'foo': [Xi(1), Xi(2), Xi(3), Xi(4)]},
                {'bar': [Xi(1), Xi(2)]},
                {'baz': None},
                {'xuz': []},
            ],
            (10, 20): [
                {'moo': [Xi(3), Xi(4), Xi(5), Xi(6)]},
            ],
        },
        {
            (30, 42131): [
                {'xfoo': [Xi(3), Xi(2), Xi(300), Xi(1012012)]},
                {'iasiqwoqwidfaoiwqh': [Xi(3120120), Xi(34894891892398)]},
                {'ieqieiai': None},
                {'uidafjaaoz': []},
            ],
            (3192, 12321): [
                {'moo': [Xi(3), Xi(4), Xi(5), Xi(6)]},
            ],
        },
    ],
    expected_result=[
        {
            (0, 1): [
                {'foo': {X(1), X(2), X(3), X(4)}},
                {'bar': {X(1), X(2)}},
                {'baz': None},
                {'xuz': set()},
            ],
            (10, 20): [
                {'moo': {X(3), X(4), X(5), X(6)}},
            ],
        },
        {
            (30, 42131): [
                {'xfoo': {X(3), X(2), X(300), X(1012012)}},
                {'iasiqwoqwidfaoiwqh': {X(3120120), X(34894891892398)}},
                {'ieqieiai': None},
                {'uidafjaaoz': set()},
            ],
            (3192, 12321): [
                {'moo': {X(3), X(4), X(5), X(6)}},
            ],
        },
    ],
    expected_comprehension=CASE_COMPLEX1_COMP,
    expected_types={NodeType.MODEL: {X}},
)


CASE_DICT_KEY_STR_VALUE_SET_NAMEDTUPLE_Z = TypeExpressionTest(
    type_expression=Dict[str, Set[Z]],
    serialized_data={
        'foo': [[Xi(1), 2, 'foo']],
        'bar': [[Xi(3), 4, 'bar1'], [Xi(5), 6, 'bar2'], [Xi(7), 8, 'bar3']],
        'baz': [],
    },
    expected_result={
        'foo': {Z(X(1), 2, 'foo')},
        'bar': {Z(X(3), 4, 'bar1'), Z(X(5), 6, 'bar2'), Z(X(7), 8, 'bar3')},
        'baz': set(),
    },
    expected_comprehension=None,
    expected_types={NodeType.MODEL: {X}},
)


CASE_SCALAR_STR = TypeExpressionTest(
    type_expression=str,
    serialized_data='foo',
    expected_result='foo',
    expected_comprehension='a',
    expected_types={},
)

CASE_OPTIONAL_SCALAR_STR = TypeExpressionTest(
    type_expression=str,
    serialized_data=None,
    expected_result=None,
    expected_comprehension='a',
    expected_types={},
)

CASE_SCALAR_INT = TypeExpressionTest(
    type_expression=int,
    serialized_data=100,
    expected_result=100,
    expected_comprehension='a',
    expected_types={},
)

CASE_LIST_INT = TypeExpressionTest(
    type_expression=List[int],
    serialized_data=[1, 2, 3, 4],
    expected_result=[1, 2, 3, 4],
    expected_comprehension='[b for b in a]',
    expected_types={},
)

CASE_LIST_DECIMAL = TypeExpressionTest(
    type_expression=List[Decimal],
    serialized_data=[
        str(Decimal('1.12')),
        str(Decimal('2.23')),
        str(Decimal('3.34')),
        str(Decimal('4.45')),
    ],
    expected_result=[
        Decimal('1.12'),
        Decimal('2.23'),
        Decimal('3.34'),
        Decimal('4.45'),
    ],
    expected_comprehension='[_Decimal_(b) for b in a]',
    expected_types={NodeType.DECIMAL: {Decimal}},
)

CASE_MAPPING_KEY_DATETIME_VALUE_OPTIONAL_LIST_DATETIME = TypeExpressionTest(
    type_expression=Mapping[datetime, Optional[List[datetime]]],
    serialized_data={
        datetime(2019, 12, 16, 19, 45, 18, 162288).isoformat(): [
            datetime(2017, 12, 16, 19, 45, 18, 162288).isoformat(),
            datetime(2018, 12, 16, 19, 45, 18, 162288).isoformat(),
        ],
        datetime(2019, 12, 15, 11, 45, 18, 9391).isoformat(): None,
        datetime(2018, 12, 15, 11, 45, 18, 9391).isoformat(): [],
    },
    expected_result={
        datetime(2019, 12, 16, 19, 45, 18, 162288): [
            datetime(2017, 12, 16, 19, 45, 18, 162288),
            datetime(2018, 12, 16, 19, 45, 18, 162288),
        ],
        datetime(2019, 12, 15, 11, 45, 18, 9391): None,
        datetime(2018, 12, 15, 11, 45, 18, 9391): [],
    },
    expected_comprehension='''\
{_iso8601_parse_(b): \
([_iso8601_parse_(d) for d in c] if c is not None else None) \
for b, c in a.items()}''',
    expected_types={NodeType.DATETIME: {datetime}},
)

CASE_LIST_OPTIONAL_INT = TypeExpressionTest(
    type_expression=List[Optional[int]],
    serialized_data=[1, 2, 3, None, 4],
    expected_result=[1, 2, 3, None, 4],
    expected_comprehension='[(b if b is not None else None) for b in a]',
    expected_types={},
)

CASE_SCALAR_MODEL = TypeExpressionTest(
    type_expression=X,
    serialized_data=X(10).dumps(),
    expected_result=X(10),
    expected_comprehension='test__X._from_data_field(a)',
    expected_types={NodeType.MODEL: {X}},
)

CASE_SCALAR_OPTIONAL_MODEL = TypeExpressionTest(
    type_expression=Optional[X],
    serialized_data=X(10).dumps(),
    expected_result=X(10),
    expected_comprehension='''\
(test__X._from_data_field(a) if a is not None else None)''',
    expected_types={NodeType.MODEL: {X}},
)

CASE_SCALAR_ABSTRACT_MODEL = TypeExpressionTest(
    type_expression=ModelT,
    serialized_data=X(10).dumps(),
    expected_result=X(10),
    expected_comprehension='_Model_._from_data_field(a)',
    expected_types={NodeType.MODEL: {ModelT}},
)

CASE_SCALAR_OPTIONAL_ABSTRACT_MODEL = TypeExpressionTest(
    type_expression=Union[ModelT, None],
    serialized_data=X(10).dumps(),
    expected_result=X(10),
    expected_comprehension='''\
(_Model_._from_data_field(a) if a is not None else None)''',
    expected_types={NodeType.MODEL: {ModelT}},
)

CASE_SCALAR_DECIMAL = TypeExpressionTest(
    type_expression=Decimal,
    serialized_data=str(Decimal('120213.123123')),
    expected_result=Decimal('120213.123123'),
    expected_comprehension='_Decimal_(a)',
    expected_types={NodeType.DECIMAL: {Decimal}},
)

NOW = datetime.utcnow()
CASE_SCALAR_DATETIME = TypeExpressionTest(
    type_expression=datetime,
    serialized_data=NOW.isoformat(),
    expected_result=NOW,
    expected_comprehension='_iso8601_parse_(a)',
    expected_types={NodeType.DATETIME: {datetime}},
)

CASE_TUPLE_SINGLE_ELEMENT = TypeExpressionTest(
    type_expression=Tuple[int],
    serialized_data=[1, 2, 3],
    expected_result=(1,),
    expected_comprehension='(a[0],)',
    expected_types={},
)


CASE_TUPLE_X_VARARGS = TypeExpressionTest(
    type_expression=Tuple[X, ...],
    serialized_data=[Xi(1), Xi(2), Xi(3)],
    expected_result=(X(1), X(2), X(3)),
    expected_comprehension='tuple(test__X._from_data_field(b) for b in a)',
    expected_types={NodeType.MODEL: {X}},
)

CASE_TUPLE_NO_ARGS = TypeExpressionTest(
    type_expression=tuple,
    serialized_data=[1, 2, 3],
    expected_result=(1, 2, 3),
    expected_comprehension='tuple(a)',
    expected_types={},
)

CASE_LIST_NO_ARGS = TypeExpressionTest(
    type_expression=list,
    serialized_data=[1, 2, 3],
    expected_result=[1, 2, 3],
    expected_comprehension='list(a)',
    expected_types={},
)

CASE_UNION_STR_INT_FLOAT = TypeExpressionTest(
    type_expression=Union[str, int, float],
    serialized_data='foo',
    expected_result='foo',
    expected_comprehension='a',
    expected_types={},
)

CASE_UNION_X_Y = TypeExpressionTest(
    type_expression=Union[X, Y],
    serialized_data=Y('foo').dumps(),
    expected_result=Y('foo'),
    expected_comprehension='_Model_._from_data_field(a)',
    expected_types={NodeType.MODEL: {ModelT}},
)

CASES = [
    CASE_TUPLE_LIST_SET_X,
    CASE_LIST_LIST_X,
    CASE_DICT_KEY_STR_VALUE_OPTIONAL_SET_X,
    CASE_DICT_KEY_STR_VALUE_OPTIONAL_SET_ABSMODEL,
    CASE_COMPLEX1,
    CASE_DICT_KEY_STR_VALUE_SET_NAMEDTUPLE_Z,
    CASE_SCALAR_STR,
    CASE_OPTIONAL_SCALAR_STR,
    CASE_SCALAR_INT,
    CASE_LIST_INT,
    CASE_LIST_DECIMAL,
    CASE_MAPPING_KEY_DATETIME_VALUE_OPTIONAL_LIST_DATETIME,
    CASE_LIST_OPTIONAL_INT,
    CASE_SCALAR_MODEL,
    CASE_SCALAR_OPTIONAL_MODEL,
    CASE_SCALAR_ABSTRACT_MODEL,
    CASE_SCALAR_OPTIONAL_ABSTRACT_MODEL,
    CASE_SCALAR_DECIMAL,
    CASE_SCALAR_DATETIME,
    CASE_TUPLE_SINGLE_ELEMENT,
    CASE_TUPLE_X_VARARGS,
    CASE_TUPLE_NO_ARGS,
    CASE_LIST_NO_ARGS,
    CASE_UNION_STR_INT_FLOAT,
    CASE_UNION_X_Y,
]


@pytest.mark.parametrize('case', CASES)
def test_comprehension(case):
    expr = TypeExpression(case.type_expression)
    if case.expected_comprehension:
        assert expr.as_comprehension() == case.expected_comprehension


@pytest.mark.parametrize('case', CASES)
def test_compile(case):
    expr = TypeExpression(case.type_expression)
    fun = expr.as_function(globals=globals())
    assert fun(case.serialized_data) == case.expected_result
    assert expr.found_types == case.expected_types
