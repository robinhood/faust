"""Models used by agents internally."""
from typing import Any
from faust.models import Record
from faust.types import K, ModelT

__all__ = ['ReqRepRequest', 'ReqRepResponse']


class ReqRepRequest(Record,
                    serializer='json',
                    namespace='@ReqRepRequest',  # internal namespace
                    # any stream should allow this type
                    # to wrap other values.
                    allow_blessed_key=True):
    """Value wrapped in a Request-Reply request."""

    # agent.ask(value) wraps the value in this record
    # so that the receiving agent knows where to send the reply.

    value: Any
    reply_to: str
    correlation_id: str


class ModelReqRepRequest(ReqRepRequest):
    """Request-Reply request where value is a model."""

    value: ModelT


class ReqRepResponse(Record, serializer='json', namespace='@ReqRepResponse'):
    """Request-Reply response."""

    key: K
    value: Any
    correlation_id: str


class ModelReqRepResponse(ReqRepResponse):
    value: Any
