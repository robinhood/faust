import re
from typing import MutableMapping, Pattern, Sequence, Tuple, Type, Union
from .types import CodecArg, Topic

__all__ = [
    'topic',
    'topic_from_topic',
    'topic_to_map',
    'get_uniform_topic_type',
]


def topic(*topics: str,
          pattern: Union[str, Pattern] = None,
          key_type: Type = None,
          value_type: Type = None,
          key_serializer: CodecArg = None) -> Topic:
    """Define new topic.

    Arguments:
        *topics: str:  List of topic names.

    Keyword Arguments:
        pattern (Union[str, Pattern]): Regular expression to match.
            You cannot specify both topics and a pattern.
        key_type (Type): Model used for keys in this topic.
        value_type (Type): Model used for values in this topic.

    Raises:
        TypeError: if both `topics` and `pattern` is provided.

    Returns:
        faust.types.Topic: a named tuple.

    """
    if pattern and topics:
        raise TypeError('Cannot specify both topics and pattern.')
    if isinstance(pattern, str):
        pattern = re.compile(pattern)

    return Topic(
        topics=topics,
        pattern=pattern,
        key_type=key_type,
        value_type=value_type,
    )


def topic_from_topic(topic: Topic,
                     *,
                     prefix: str = '',
                     suffix: str = '',
                     format: str = '{prefix}{topic}{suffix}') -> Topic:
    if topic.pattern:
        raise ValueError('Cannot add suffix to Topic with pattern')
    return Topic(
        topics=[
            format.format(prefix=prefix, topic=topic, suffix=suffix)
            for topic in topic.topics
        ],
        pattern=topic.pattern,
        key_type=topic.key_type,
        value_type=topic.value_type,
    )


def topic_to_map(topics: Sequence[Topic]) -> MutableMapping[str, Topic]:
    return {
        s: topic
        for topic in topics for s in _subtopics_for(topic)
    }


def _subtopics_for(topic: Topic) -> Sequence[str]:
    return [topic.pattern.pattern] if topic.pattern else topic.topics


def get_uniform_topic_type(topics: Sequence[Topic]) -> Tuple[Type, Type]:
    key_type: Type = None
    value_type: Type = None
    for topic in topics:
        if key_type is None:
            key_type = topic.key_type
        else:
            if topic.key_type is not key_type:
                raise TypeError(
                    'Cannot get type from topics having differing types.')
            assert topic.key_type is key_type
        if value_type is None:
            value_type = topic.value_type
        else:
            if topic.value_type is not value_type:
                raise TypeError(
                    'Cannot get type from topics having differing types.')
    return key_type, value_type
