import re
from typing import Callable, Pattern, Union
from .types import Serializer, Topic


def topic(*topics: str,
          pattern: Union[str, Pattern] = None,
          type: type = None,
          key_serializer: Serializer = None,
          value_serializer: Serializer = None) -> Topic:
    if isinstance(pattern, str):
        pattern = re.compile(pattern)
    return Topic(
        topics=topics,
        pattern=pattern,
        type=type,
        key_serializer=key_serializer,
        value_serializer=value_serializer,
    )
