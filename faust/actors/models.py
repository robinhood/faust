from ..models import Record
from ..types import K, ModelT

__all__ = ['ReqRepRequest', 'ReqRepResponse']


# XXX "namespace" below is the Avro registry key.
# It's going to be included in every single actor request,
# so I figured "org.faust.ReqRepRequest" was too long,
# but maybe that's silly?
class ReqRepRequest(Record, serializer='json', namespace='@RRReq'):
    """Value wrapped in a Request-Reply request."""

    # actor.ask(value) wraps the value in this record
    # so that the receiving actor knows where to send the reply.

    value: ModelT
    reply_to: str
    correlation_id: str


class ReqRepResponse(Record, serializer='json', namespace='@RRRes'):
    """Request-Reply response."""

    key: K
    value: ModelT
    correlation_id: str
