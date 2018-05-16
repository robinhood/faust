"""Cluster assignement."""
from typing import List, MutableMapping, Sequence, Set, cast
from faust.models import Record
from .client_assignment import (
    ClientAssignment,
    ClientMetadata,
    CopartitionedAssignment,
)

__all__ = ['CopartMapping', 'ClusterAssignment']

CopartMapping = MutableMapping[str, CopartitionedAssignment]


class ClusterAssignment(Record,
                        serializer='json',
                        include_metadata=False,
                        namespace='@ClusterAssignment'):
    """Cluster assignment state."""

    # These are optional, but should never be set to None
    subscriptions: MutableMapping[str, Sequence[str]] = cast(
        MutableMapping[str, Sequence[str]], None)
    assignments: MutableMapping[str, ClientAssignment] = cast(
        MutableMapping[str, ClientAssignment], None)

    def __post_init__(self) -> None:
        if self.subscriptions is None:
            self.subscriptions = {}
        if self.assignments is None:
            self.assignments = {}

    def topics(self) -> Set[str]:
        # All topics subscribed to in the cluster
        return {topic for sub in self.subscriptions.values() for topic in sub}

    def add_client(self, client: str, subscription: List[str],
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
