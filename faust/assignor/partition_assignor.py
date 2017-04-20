from collections import defaultdict
from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from kafka.coordinator.protocol import (
    ConsumerProtocolMemberMetadata, ConsumerProtocolMemberAssignment,
)
from typing import MutableMapping, Set
from .client_assignment import ClientAssignment
from .cluster_assignment import ClusterAssignment
from .copartitioned_assignor import CopartitionedAssignor


class PartitionAssignor(AbstractPartitionAssignor):

    def __init__(self):
        super(PartitionAssignor, self).__init__()
        self._assignment = ClientAssignment(actives={}, standbys={})

    '''PartitionAssignor to handle internal topic creation

    Further, this assignor needs to be sticky and potentially redundant

    Interface copied from:
    https://github.com/dpkp/kafka-python/blob/master/kafka/coordinator/assignors/abstract.py
    '''

    def on_assignment(self, assignment):
        self._assignment: ClientAssignment = ClientAssignment.loads(
            assignment.user_data)
        assert assignment.assignment == \
            self._assignment.kafka_protocol_assignment()

    def metadata(self, topics):
        return ConsumerProtocolMemberMetadata(self.version, list(topics),
                                              self._assignment.dumps())

    @classmethod
    def _get_copartitioned_groups(cls, topics,
                                  cluster) -> MutableMapping[int, Set[str]]:
        topics_by_partitions = defaultdict(set)
        for topic in topics:
            num_partitions = len(cluster.partitions_for_topic(topic))
            topics_by_partitions[num_partitions].add(topic)
        return topics_by_partitions

    def _build_assignment(self,
                          assignments: MutableMapping[str, ClientAssignment],
                          ) -> MutableMapping[
                                str, ConsumerProtocolMemberAssignment]:
        return {
            client: ConsumerProtocolMemberAssignment(
                self.version,
                sorted(assignment.kafka_protocol_assignment()),
                assignment.dumps()
            )
            for client, assignment in assignments.items()
        }

    def assign(self, cluster,
               member_metadata: MutableMapping[str,
                                               ConsumerProtocolMemberMetadata]
               ) -> MutableMapping[str, ConsumerProtocolMemberAssignment]:
        cluster_assgn = ClusterAssignment()
        cluster_assgn.add_clients(member_metadata)
        topics = cluster_assgn.topics()
        copartitioned_groups = self._get_copartitioned_groups(topics, cluster)

        # Initialize fresh assignment
        assignments = defaultdict(ClientAssignment)

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
            for client, assgn in assignor.get_assignment().items():
                assignments[client].add_copartitioned_assignment(assgn)

        return self._build_assignment(assignments)

    @property
    def name(self):
        return "faust"

    @property
    def version(self):
        return 1
