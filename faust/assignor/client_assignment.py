from typing import MutableMapping, Sequence, Set
from faust.models import Record


class CopartitionedAssignment(object):
    actives: Set[int]
    standbys: Set[int]
    topics: Set[str]

    def __init__(self, actives: Set[int]=None, standbys: Set[int]=None,
                 topics: Set[str]=None) -> None:
        self.actives = actives or set()
        self.standbys = standbys or set()
        self.topics = topics or set()

    def validate(self):
        if not self.actives.isdisjoint(self.standbys):
            raise ValueError("Actives and Standbys are disjoint")

    def num_assigned(self, active: bool) -> int:
        return len(self.get_assigned_partitions(active))

    def get_unassigned(self, num_partitions: int, active: bool) -> Set[int]:
        partitions = self.get_assigned_partitions(active)
        all_partitions = set(range(num_partitions))
        assert partitions.issubset(all_partitions)
        return all_partitions.difference(partitions)

    def pop_partition(self, active: bool) -> int:
        return self.get_assigned_partitions(active).pop()

    def unassign_partition(self, partition: int, active: bool) -> None:
        return self.get_assigned_partitions(active).remove(partition)

    def assign_partition(self, partition: int, active: bool) -> None:
        self.get_assigned_partitions(active).add(partition)

    def unassign_extras(self, capacity: int, replicas: int) -> None:
        while len(self.actives) > capacity:
            self.actives.pop()
        while len(self.standbys) > capacity * replicas:
            self.standbys.pop()

    def partition_assigned(self, partition: int, active: bool) -> bool:
        return partition in self.get_assigned_partitions(active)

    def promote_standby_to_active(self, standby_partition: int) -> None:
        assert standby_partition in self.standbys, "Not standby for partition"
        self.standbys.remove(standby_partition)
        self.actives.add(standby_partition)

    def get_assigned_partitions(self, active: bool) -> Set[int]:
        return self.actives if active else self.standbys

    def can_assign(self, partition: int, active: bool) -> bool:
        return (
            not self.partition_assigned(partition, active) and
            (active or not self.partition_assigned(partition, active=True))
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}<actives={self.actives}, " \
               f"standbys={self.standbys}, topics={self.topics}>"


class ClientAssignment(Record, serializer="json"):
    actives: MutableMapping[str, Sequence[int]]  # Topic -> Partition
    standbys: MutableMapping[str, Sequence[int]]  # Topic -> Partition

    def kafka_protocol_assignment(self):
        return [(topic, list(partitions))
                for topic, partitions in self.actives.items()]

    def add_copartitioned_assignment(self,
                                     assignment: CopartitionedAssignment
                                     ) -> None:
        assert not any(topic in self.actives or topic in self.standbys
                       for topic in assignment.topics)
        for topic in assignment.topics:
            self.actives[topic] = assignment.actives
            self.standbys[topic] = assignment.standbys

    def copartitioned_assignment(self,
                                 topics: Set[str]) -> CopartitionedAssignment:
        assignment = CopartitionedAssignment(
            actives=self._colocated_partitions(topics, active=True),
            standbys=self._colocated_partitions(topics, active=False),
            topics=topics,
        )
        assignment.validate()
        return assignment

    def _colocated_partitions(self, topics: Set[str],
                              active: bool) -> Set[int]:
        assignment = self.actives if active else self.standbys
        # We take the first partition set for a topic which has a valid
        # assignment assuming subscription changes with co-partitioned topic
        # groups will be rare.
        topic_assignments = [set(assignment.get(topic, list()))
                             for topic in topics]
        return next((
            partitions
            for partitions in topic_assignments
            if len(partitions) > 0
        ), set())
