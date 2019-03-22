"""Client Assignment."""
import copy
from typing import List, Mapping, MutableMapping, Sequence, Set, Tuple, cast
from faust.models import Record
from faust.types import TP
from faust.types.assignor import HostToPartitionMap
from faust.types.tables import TableManagerT

R_COPART_ASSIGNMENT = '''
<{name} actives={self.actives} standbys={self.standbys} topics={self.topics}>
'''.strip()


class CopartitionedAssignment:
    """Copartitioned Assignment."""

    actives: Set[int]
    standbys: Set[int]
    topics: Set[str]

    def __init__(self,
                 actives: Set[int] = None,
                 standbys: Set[int] = None,
                 topics: Set[str] = None) -> None:
        self.actives = actives or set()
        self.standbys = standbys or set()
        self.topics = topics or set()

    def validate(self) -> None:
        if not self.actives.isdisjoint(self.standbys):
            self.standbys.difference_update(self.actives)

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
        assert standby_partition in self.standbys, 'Not standby for partition'
        self.standbys.remove(standby_partition)
        self.actives.add(standby_partition)

    def get_assigned_partitions(self, active: bool) -> Set[int]:
        return self.actives if active else self.standbys

    def can_assign(self, partition: int, active: bool) -> bool:
        return (not self.partition_assigned(partition, active) and
                (active or
                 not self.partition_assigned(partition, active=True)))

    def __repr__(self) -> str:
        return R_COPART_ASSIGNMENT.format(
            name=type(self).__name__,
            self=self,
        )


class ClientAssignment(Record,
                       serializer='json',
                       include_metadata=False,
                       namespace='@ClientAssignment'):
    """Client Assignment data model."""

    actives: MutableMapping[str, List[int]]   # Topic -> Partition
    standbys: MutableMapping[str, List[int]]  # Topic -> Partition

    @property
    def active_tps(self) -> Set[TP]:
        return self._get_tps(active=True)

    @property
    def standby_tps(self) -> Set[TP]:
        return self._get_tps(active=False)

    def _get_tps(self, active: bool) -> Set[TP]:
        assignment = self.actives if active else self.standbys
        return {
            TP(topic=topic, partition=partition)
            for topic, partitions in assignment.items()
            for partition in partitions
        }

    def kafka_protocol_assignment(
            self,
            table_manager: TableManagerT) -> Sequence[Tuple[str, List[int]]]:
        assignment: MutableMapping[str, List[int]] = copy.deepcopy(
            self.actives)
        for topic, partitions in self.standbys.items():
            if topic in table_manager.changelog_topics:
                if topic not in assignment:
                    assignment[topic] = []
                assignment[topic].extend(partitions)
        return list(assignment.items())

    def add_copartitioned_assignment(
            self, assignment: CopartitionedAssignment) -> None:
        assigned = set(self.actives.keys()).union(set(self.standbys.keys()))
        assert not any(topic in assigned for topic in assignment.topics)
        for topic in assignment.topics:
            self.actives[topic] = list(assignment.actives)
            self.standbys[topic] = list(assignment.standbys)

    def copartitioned_assignment(
            self, topics: Set[str]) -> CopartitionedAssignment:
        assignment = CopartitionedAssignment(
            actives=self._colocated_partitions(topics, active=True),
            standbys=self._colocated_partitions(topics, active=False),
            topics=topics,
        )
        assignment.validate()
        return assignment

    def _colocated_partitions(
            self, topics: Set[str], active: bool) -> Set[int]:
        assignment = self.actives if active else self.standbys
        # We take the first partition set for a topic which has a valid
        # assignment assuming subscription changes with co-partitioned topic
        # groups will be rare.
        topic_assignments = (set(assignment.get(t, set())) for t in topics)
        valid_partitions = (p for p in topic_assignments if p)
        return next(valid_partitions, set())


class ClientMetadata(Record,
                     serializer='json',
                     include_metadata=False,
                     namespace='@ClientMetadata'):
    """Client Metadata data model."""

    assignment: ClientAssignment
    url: str
    changelog_distribution: HostToPartitionMap
    topic_groups: Mapping[str, int] = cast(Mapping[str, int], None)

    def __post_init__(self) -> None:
        if self.topic_groups is None:
            self.topic_groups = {}
