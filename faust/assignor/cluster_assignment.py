from typing import Any, List, MutableMapping, Sequence, Set
from .client_assignment import (
    ClientAssignment, ClientMetadata, CopartitionedAssignment,
)
from ..models import Record

CopartMapping = MutableMapping[str, CopartitionedAssignment]


class ClusterAssignment(Record,
                        serializer='json',
                        include_metadata=False,
                        namespace='@ClusterAssignment'):
    subscriptions: MutableMapping[str, Sequence[str]] = None
    assignments: MutableMapping[str, ClientAssignment] = None

    def __init__(self,
                 subscriptions: MutableMapping[str, Sequence[str]] = None,
                 assignments: MutableMapping[str, ClientAssignment] = None,
                 **kwargs: Any) -> None:
        super().__init__(
            subscriptions=subscriptions or {},
            assignments=assignments or {},
            **kwargs)

    def topics(self) -> Set[str]:
        # All topics subscribed to in the cluster
        return {
            topic
            for sub in self.subscriptions.values()
            for topic in sub
        }

    def add_client(self, client: str,
                   subscription: List[str],
                   metadata: ClientMetadata) -> None:
        self.subscriptions[client] = list(subscription)
        self.assignments[client] = metadata.assignment

    def copartitioned_assignments(
            self, copartitioned_topics: Set[str]) -> CopartMapping:
        # We only pick clients that subscribe to all copartitioned topics
        subscribed_clis = {
            cli for cli, sub in self.subscriptions.items()
            if copartitioned_topics.issubset(sub)
        }
        return {
            cli: assignment.copartitioned_assignment(copartitioned_topics)
            for cli, assignment in self.assignments.items()
            if cli in subscribed_clis
        }
