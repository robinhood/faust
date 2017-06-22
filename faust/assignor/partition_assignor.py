from collections import defaultdict
from kafka.cluster import ClusterMetadata
from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from kafka.coordinator.protocol import (
    ConsumerProtocolMemberMetadata, ConsumerProtocolMemberAssignment,
)
from logging import getLogger
from typing import Iterable, MutableMapping, Sequence, Set, cast
from .client_assignment import ClientAssignment
from .cluster_assignment import ClusterAssignment
from .copartitioned_assignor import CopartitionedAssignor

MemberAssignmentMapping = MutableMapping[str, ConsumerProtocolMemberAssignment]
MemberMetadataMapping = MutableMapping[str, ConsumerProtocolMemberMetadata]
ClientAssignmentMapping = MutableMapping[str, ClientAssignment]
CopartitionedGroups = MutableMapping[int, Iterable[Set[str]]]

logger = getLogger(__name__)


class PartitionAssignor(AbstractPartitionAssignor):
    """PartitionAssignor handles internal topic creation.

    Further, this assignor needs to be sticky and potentially redundant

    Interface copied from:
    https://github.com/dpkp/kafka-python/blob/master/
        kafka/coordinator/assignors/abstract.py
    """
    _assignment: ClientAssignment

    def __init__(self) -> None:
        super().__init__()
        self._assignment = ClientAssignment(actives={}, standbys={})

    def on_assignment(
            self, assignment: ConsumerProtocolMemberMetadata) -> None:
        self._assignment = cast(ClientAssignment,
                                ClientAssignment.loads(assignment.user_data))
        print(self._assignment)
        a = sorted(assignment.assignment)
        b = sorted(self._assignment.kafka_protocol_assignment())
        assert a == b, f'{a!r} != {b!r}'

    def metadata(self, topics: Set[str]) -> ConsumerProtocolMemberMetadata:
        return ConsumerProtocolMemberMetadata(
            self.version, list(topics), self._assignment.dumps())

    @classmethod
    def _group_co_subscribed(cls, topics: Set[str],
                             member_metadata: MemberMetadataMapping
                             ) -> Iterable[Set[str]]:
        topic_subscriptions: MutableMapping[str, Set[str]] = defaultdict(set)
        for client, metadata in member_metadata.items():
            for topic in metadata.subscription:
                topic_subscriptions[topic].add(client)
        co_subscribed: MutableMapping[Sequence[str], Set[str]] = defaultdict(
            set)
        for topic in topics:
            clients = topic_subscriptions[topic]
            assert clients, "Subscribed clients for topic cannot be empty"
            co_subscribed[tuple(clients)].add(topic)
        return co_subscribed.values()

    @classmethod
    def _get_copartitioned_groups(
            cls, topics: Set[str],
            cluster: ClusterMetadata,
            member_metadata: MemberMetadataMapping) -> CopartitionedGroups:
        topics_by_partitions: MutableMapping[int, Set] = defaultdict(set)
        for topic in topics:
            num_partitions = len(cluster.partitions_for_topic(topic) or set())
            if num_partitions == 0:
                logger.warning(f'Ignoring missing topic: {topic}')
                continue
            topics_by_partitions[num_partitions].add(topic)
        # We group copartitioned topics by subscribed clients such that
        # a group of co-subscribed topics with the same number of partitions
        # are copartitioned
        copart_grouped = {
            num_partitions: cls._group_co_subscribed(topics, member_metadata)
            for num_partitions, topics in topics_by_partitions.items()
        }
        return copart_grouped

    def assign(self, cluster: ClusterMetadata,
               member_metadata: MemberMetadataMapping
               ) -> MemberAssignmentMapping:
        cluster_assgn = ClusterAssignment()
        cluster_assgn.add_clients(member_metadata)
        topics = cluster_assgn.topics()
        copartitioned_groups = self._get_copartitioned_groups(
            topics, cluster, member_metadata)

        # Initialize fresh assignment
        assignments = {
            member_id: ClientAssignment(actives={}, standbys={})
            for member_id in member_metadata
        }

        for num_partitions, topic_groups in copartitioned_groups.items():
            for topics in topic_groups:
                assert len(topics) > 0 and num_partitions > 0
                # Get assignment for unique copartitioned group
                assgn = cluster_assgn.copartitioned_assignments(topics)
                assignor = CopartitionedAssignor(
                    topics=topics,
                    cluster_asgn=assgn,
                    num_partitions=num_partitions,
                )
                # Update client assignments for copartitioned group
                for client, copart_assn in assignor.get_assignment().items():
                    assignments[client].add_copartitioned_assignment(
                        copart_assn)

        res = self._protocol_assignments(assignments)
        print(res)
        return res

    def _protocol_assignments(
            self,
            assignments: ClientAssignmentMapping) -> MemberAssignmentMapping:
        return {
            client: ConsumerProtocolMemberAssignment(
                self.version,
                sorted(assignment.kafka_protocol_assignment()),
                assignment.dumps()
            )
            for client, assignment in assignments.items()
        }

    @property
    def name(self) -> str:
        return 'faust'

    @property
    def version(self) -> int:
        return 1
