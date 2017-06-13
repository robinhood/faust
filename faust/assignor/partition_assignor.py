from collections import defaultdict
from kafka.cluster import ClusterMetadata
from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from kafka.coordinator.protocol import (
    ConsumerProtocolMemberMetadata, ConsumerProtocolMemberAssignment,
)
from typing import MutableMapping, Set, cast
from .client_assignment import ClientAssignment
from .cluster_assignment import ClusterAssignment
from .copartitioned_assignor import CopartitionedAssignor

MemberAssignmentMapping = MutableMapping[str, ConsumerProtocolMemberAssignment]
ClientAssignmentMapping = MutableMapping[str, ClientAssignment]


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
        a = sorted(assignment.assignment)
        b = sorted(self._assignment.kafka_protocol_assignment())
        assert a == b, f'{a!r} != {b!r}'

    def metadata(self, topics: Set[str]) -> ConsumerProtocolMemberMetadata:
        return ConsumerProtocolMemberMetadata(
            self.version, list(topics), self._assignment.dumps())

    @classmethod
    def _get_copartitioned_groups(
            cls, topics: Set[str],
            cluster: ClusterMetadata) -> MutableMapping[int, Set[str]]:
        topics_by_partitions: MutableMapping[int, Set] = defaultdict(set)
        for topic in topics:
            num_partitions = len(cluster.partitions_for_topic(topic) or set())
            topics_by_partitions[num_partitions].add(topic)
        return topics_by_partitions

    def assign(self, cluster: ClusterMetadata,
               member_metadata: MutableMapping[str,
                                               ConsumerProtocolMemberMetadata]
               ) -> MemberAssignmentMapping:
        cluster_assgn = ClusterAssignment()
        cluster_assgn.add_clients(member_metadata)
        topics = cluster_assgn.topics()
        copartitioned_groups = self._get_copartitioned_groups(topics, cluster)

        # Initialize fresh assignment
        assignments = {
            member_id: ClientAssignment(actives={}, standbys={})
            for member_id in member_metadata
        }

        for num_partitions, topics in copartitioned_groups.items():
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
                assignments[client].add_copartitioned_assignment(copart_assn)

        return self._protocol_assignments(assignments)

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
