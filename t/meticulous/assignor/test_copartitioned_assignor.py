import copy
from collections import Counter
from typing import MutableMapping
from faust.assignor.client_assignment import CopartitionedAssignment
from faust.assignor.copartitioned_assignor import CopartitionedAssignor
from hypothesis import assume, given, settings
from hypothesis.strategies import integers

TEST_DEADLINE = 4000


_topics = {'foo', 'bar', 'baz'}


def is_valid(cli_assignments: MutableMapping[str, CopartitionedAssignment],
             num_partitions: int, replicas: int) -> bool:
    all_partitions = set(range(num_partitions))
    active_counts = Counter(partition
                            for assignment in cli_assignments.values()
                            for partition in assignment.actives)
    assert all(
        count == 1 for count in active_counts.values()
    ), 'Multiple clients assigned to same active'
    assert set(active_counts.keys()) == all_partitions
    standby_counts = Counter(partition
                             for assignment in cli_assignments.values()
                             for partition in assignment.standbys)
    assert all(
        count == replicas for count in standby_counts.values()
    ), 'Multiple clients assigned to same active'
    assert not replicas or set(standby_counts.keys()) == all_partitions
    return True


def client_addition_sticky(old: MutableMapping[str, CopartitionedAssignment],
                           new: MutableMapping[str, CopartitionedAssignment],
                           ) -> bool:
    assert all(
        partition in old[client].actives
        for client in old
        for partition in new[client].actives
    )
    return True


def client_removal_sticky(old: MutableMapping[str, CopartitionedAssignment],
                          new: MutableMapping[str, CopartitionedAssignment],
                          ) -> bool:
    removed = set(old).difference(set(new))
    reassigned_partitions = {partition
                             for client, assignment in old.items()
                             for partition in assignment.actives
                             if client in removed}
    assert all(
        partition in old[client].actives or partition in reassigned_partitions
        for client in new
        for partition in new[client].actives
    )
    return True


@given(partitions=integers(min_value=0, max_value=256),
       replicas=integers(min_value=0, max_value=64),
       num_clients=integers(min_value=1, max_value=1024))
@settings(deadline=TEST_DEADLINE)
def test_fresh_assignment(partitions, replicas, num_clients):
    assume(replicas < num_clients)
    client_assignments = {
        str(client): CopartitionedAssignment(topics=_topics)
        for client in range(num_clients)
    }
    new_assignments = CopartitionedAssignor(
        _topics, client_assignments,
        partitions, replicas=replicas).get_assignment()
    assert is_valid(new_assignments, partitions, replicas)


@given(partitions=integers(min_value=0, max_value=256),
       replicas=integers(min_value=0, max_value=64),
       num_clients=integers(min_value=1, max_value=1024),
       num_additional_clients=integers(min_value=1, max_value=16))
@settings(deadline=TEST_DEADLINE)
def test_add_new_clients(partitions, replicas, num_clients,
                         num_additional_clients):
    assume(replicas < num_clients and num_additional_clients < num_clients)
    client_assignments = {
        str(client): CopartitionedAssignment(topics=_topics)
        for client in range(num_clients)
    }
    valid_assignment = CopartitionedAssignor(
        _topics, client_assignments,
        partitions, replicas=replicas).get_assignment()
    old_assignments = copy.deepcopy(valid_assignment)

    # adding fresh clients
    for client in range(num_clients, num_clients + num_additional_clients):
        valid_assignment[str(client)] = CopartitionedAssignment(topics=_topics)

    new_assignments = CopartitionedAssignor(
        _topics, valid_assignment, partitions, replicas=replicas,
    ).get_assignment()

    assert is_valid(new_assignments, partitions, replicas)
    assert client_addition_sticky(old_assignments, new_assignments)


@given(partitions=integers(min_value=0, max_value=256),
       replicas=integers(min_value=0, max_value=64),
       num_clients=integers(min_value=1, max_value=1024),
       num_removal_clients=integers(min_value=1, max_value=16))
@settings(deadline=TEST_DEADLINE)
def test_remove_clients(partitions, replicas, num_clients,
                        num_removal_clients):
    assume(num_removal_clients < num_clients and
           replicas < (num_clients - num_removal_clients))
    client_assignments = {
        str(client): CopartitionedAssignment(topics=_topics)
        for client in range(num_clients)
    }
    valid_assignment = CopartitionedAssignor(
        _topics, client_assignments,
        partitions, replicas=replicas).get_assignment()
    old_assignments = copy.deepcopy(valid_assignment)

    # adding fresh clients
    for client in range(num_clients - num_removal_clients, num_clients):
        del valid_assignment[str(client)]

    new_assignments = CopartitionedAssignor(
        _topics, valid_assignment, partitions, replicas=replicas,
    ).get_assignment()

    assert is_valid(new_assignments, partitions, replicas)
    assert client_removal_sticky(old_assignments, new_assignments)
