from kafka.coordinator.protocol import ConsumerProtocolMemberMetadata
from typing import MutableMapping, Sequence, Set, cast
from faust.models import Record
from .client_assignment import ClientAssignment, CopartitionedAssignment

MetadataMapping = MutableMapping[str, ConsumerProtocolMemberMetadata]
CopartMapping = MutableMapping[str, CopartitionedAssignment]


class ClusterAssignment(Record, serializer='json'):
    subscriptions: MutableMapping[str, Sequence[str]] = None
    assignments: MutableMapping[str, ClientAssignment] = None

    def topics(self) -> Set[str]:
        # All topics subscribed to in the cluster
        return {
            topic
            for sub in self.subscriptions.values()
            for topic in sub
        }

    def add_clients(self, client_metadata: MetadataMapping) -> None:
        for client, metadata in client_metadata.items():
            self._add_client_assignment(client, metadata)

    def _add_client_assignment(
            self, client: str,
            metadata: ConsumerProtocolMemberMetadata) -> None:
        self.subscriptions[client] = list(metadata.subscription)
        self.assignments[client] = (
            cast(ClientAssignment, ClientAssignment.loads(metadata.user_data))
            if metadata.user_data
            else ClientAssignment(actives={}, standbys={})
        )

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
