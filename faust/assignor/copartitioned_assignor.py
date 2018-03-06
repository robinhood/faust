"""Copartitioned Assignor."""
from itertools import cycle
from math import ceil
from typing import Iterable, Iterator, MutableMapping, Optional, Sequence, Set
from mode.utils.compat import Counter
from .client_assignment import CopartitionedAssignment

__all__ = ['CopartitionedAssignor']


class CopartitionedAssignor:
    """Copartitioned Assignor.

    All copartitioned topics must have the same number of partitions

    The assignment is sticky which uses the following heuristics:

    - Maintain existing assignments as long as within capacity for each client
    - Assign actives to standbys when possible (within capacity)
    - Assign in order to fill capacity of the clients

    We optimize for not over utilizing resources instead of under-utilizing
    resources. This results in a balanced assignment when capacity is the
    default value which is ``ceil(num partitions / num clients)``

    Notes:
        Currently we raise an exception if number of clients is not enough
        for the desired `replication`.
    """

    capacity: int
    num_partitions: int
    replicas: int
    topics: Set[str]

    _num_clients: int
    _client_assignments: MutableMapping[str, CopartitionedAssignment]

    def __init__(self,
                 topics: Iterable[str],
                 cluster_asgn: MutableMapping[str, CopartitionedAssignment],
                 num_partitions: int,
                 replicas: int,
                 capacity: int = None) -> None:
        self._num_clients = len(cluster_asgn)
        assert self._num_clients, "Should assign to at least 1 client"
        self.num_partitions = num_partitions
        self.replicas = min(replicas, self._num_clients - 1)
        self.capacity = (
            int(ceil(float(self.num_partitions) / self._num_clients))
            if capacity is None else capacity
        )
        self.topics = set(topics)

        assert self.capacity * self._num_clients >= self.num_partitions, \
            'Not enough capacity'

        self._client_assignments = cluster_asgn

    def get_assignment(self) -> MutableMapping[str, CopartitionedAssignment]:
        for copartitioned in self._client_assignments.values():
            copartitioned.unassign_extras(self.capacity, self.replicas)
        self._assign(active=True)
        self._assign(active=False)
        return self._client_assignments

    def _all_assigned(self, active: bool) -> bool:
        assigned_counts = self._assigned_partition_counts(active)
        total_assigns = self._total_assigns_per_partition(active)
        return all(assigned_counts[partition] == total_assigns
                   for partition in range(self.num_partitions))

    def _assign(self, active: bool) -> None:
        self._unassign_overassigned(active)
        unassigned = self._get_unassigned(active)
        self._assign_round_robin(unassigned, active)
        assert self._all_assigned(active)

    def _assigned_partition_counts(self, active: bool) -> Counter[int]:
        return Counter(
            partition
            for copartitioned in self._client_assignments.values()
            for partition in copartitioned.get_assigned_partitions(active)
        )

    def _get_client_limit(self, active: bool) -> int:
        return self.capacity * self._total_assigns_per_partition(active)

    def _total_assigns_per_partition(self, active: bool) -> int:
        return 1 if active else self.replicas

    def _unassign_overassigned(self, active: bool) -> None:
        # There are cases when multiple clients could have the same
        # assignment (zombies).  We need to handle that appropriately.
        partition_counts = self._assigned_partition_counts(active)
        total_assigns = self._total_assigns_per_partition(active=active)
        for partition in range(self.num_partitions):
            extras = partition_counts[partition] - total_assigns
            for _ in range(extras):
                assgn = next(
                    assgn
                    for assgn in self._client_assignments.values()
                    if assgn.partition_assigned(partition, active=active)
                )
                assgn.unassign_partition(partition, active=active)

    def _get_unassigned(self, active: bool) -> Sequence[int]:
        partition_counts = self._assigned_partition_counts(active)
        total_assigns = self._total_assigns_per_partition(active=active)
        assert all(
            partition_counts[partition] <= total_assigns
            for partition in range(self.num_partitions)
        )
        return [
            partition
            for partition in range(self.num_partitions)
            for _ in range(total_assigns - partition_counts[partition])
        ]

    def _can_assign(self, assignment: CopartitionedAssignment, partition: int,
                    active: bool) -> bool:
        return (
            not self._client_exhausted(assignment, active) and
            assignment.can_assign(partition, active)
        )

    def _client_exhausted(self, assignemnt: CopartitionedAssignment,
                          active: bool, client_limit: int=None) -> bool:
        if client_limit is None:
            client_limit = self._get_client_limit(active)
        return assignemnt.num_assigned(active) == client_limit

    def _find_promotable_standby(self, partition: int,
                                 candidates: Iterator[CopartitionedAssignment],
                                 ) -> Optional[CopartitionedAssignment]:
        # Round robin to find standby until we make a full cycle
        for _ in range(self._num_clients):
            assignment = next(candidates)
            can_assign = (
                assignment.partition_assigned(partition, active=False) and
                self._can_assign(assignment, partition, active=True)
            )
            if can_assign:
                return assignment
        return None

    def _find_round_robin_assignable(self, partition: int,
                                     candidates: Iterator[
                                         CopartitionedAssignment],
                                     active: bool,
                                     ) -> Optional[CopartitionedAssignment]:
        # Round robin and assign until we make a full circle
        for _ in range(self._num_clients):
            assignment = next(candidates)
            if self._can_assign(assignment, partition, active):
                return assignment
        return None

    def _assign_round_robin(self, unassigned: Iterable[int],
                            active: bool) -> None:
        # We do round robin assignment as follows:
        # - For actives, we first try to assign to a standby
        # - For standby, we offset the start for round robin to evenly
        # distribute standbys for colocated actives
        # - We do round robin
        # - If no assignment found, it must be a standby and the only unfilled
        # client(s) must be actives/standbys for the partition
        # - If no assignment found, we unassign and arbitrary partition from a
        # filled assignment such that the partition can be assigned to it
        # - This guarantees eventual assignment of all partitions
        client_limit = self._get_client_limit(active)
        candidates = cycle(self._client_assignments.values())
        unassigned = list(unassigned)
        while unassigned:
            partition = unassigned.pop(0)

            assign_to = None

            if active:
                # For actives we first try to find a standby to assign to
                assign_to = self._find_promotable_standby(partition,
                                                          candidates)
                if assign_to is not None:
                    # Unassign standby which will be promoted
                    assign_to.unassign_partition(partition, active=False)
            else:
                # For standbys we offset to round robin start to shuffle
                # assignment of standbys
                for _ in range(partition):
                    next(candidates)

            assert assign_to is None or active

            assign_to = assign_to or self._find_round_robin_assignable(
                partition, candidates, active)

            # If round robin assignment didn't work then we must be
            # assigning a standby and the only un-exhausted clients
            # are actives for the partition
            assert (
                assign_to is not None or
                (
                    not active and
                    all(
                        assgn.partition_assigned(partition, active=True) or
                        assgn.partition_assigned(partition, active=False) or
                        self._client_exhausted(assgn, active, client_limit)
                        for assgn in self._client_assignments.values()
                    )
                )
            )

            # If round robin didn't work, we free up the first full standby
            # assignment to which the partition can be assigned.
            if assign_to is None:
                assign_to = next(
                    assigment
                    for assigment in self._client_assignments.values()
                    if (self._client_exhausted(assigment, active) and
                        assigment.can_assign(partition, active))
                )  # By above assertion, should never throw error
                unassigned_partition = assign_to.pop_partition(active)
                unassigned.append(unassigned_partition)

            # Assign partition
            assign_to.assign_partition(partition, active)
