import pytest
from faust.assignor.client_assignment import CopartitionedAssignment
from faust.assignor.copartitioned_assignor import CopartitionedAssignor


_topics = ["foo", "bar", "baz"]
_num_partitions = 8


@pytest.mark.parametrize("client_assignments,replicas", [
    # Fresh assignment with 1 replica
    ({
        "cli1": CopartitionedAssignment(topics=_topics, actives=set(),
                                        standbys=set()),
        "cli2": CopartitionedAssignment(topics=_topics, actives=set(),
                                        standbys=set()),
        "cli3": CopartitionedAssignment(topics=_topics, actives=set(),
                                        standbys=set()),
    }, 1),
    # Fresh assignment with 2 replicas
    ({
        "cli1": CopartitionedAssignment(topics=_topics, actives=set(),
                                        standbys=set()),
        "cli2": CopartitionedAssignment(topics=_topics, actives=set(),
                                        standbys=set()),
        "cli3": CopartitionedAssignment(topics=_topics, actives=set(),
                                        standbys=set()),
        "cli4": CopartitionedAssignment(topics=_topics, actives=set(),
                                        standbys=set()),
    }, 2),
    # Mimics adding a new client - No redundancy
    ({
        "cli1": CopartitionedAssignment(topics=_topics, standbys=set(),
                                        actives={0, 2, 3, 4}),
        "cli2": CopartitionedAssignment(topics=_topics, standbys=set(),
                                        actives={1, 5, 6, 7}),
        "cli3": CopartitionedAssignment(topics=_topics, actives=set(),
                                        standbys=set()),
    }, 0),
    # Mimics adding a new client - With redundancy
    ({
        "cli1": CopartitionedAssignment(topics=_topics,
                                        standbys={1, 5, 6, 7},
                                        actives={0, 2, 3, 4}),
        "cli2": CopartitionedAssignment(topics=_topics,
                                        standbys={0, 2, 3, 4},
                                        actives={1, 5, 6, 7}),
        "cli3": CopartitionedAssignment(topics=_topics, actives=set(),
                                        standbys=set()),
    }, 1),
    # Mimics removing a client - standbys available
    ({
        "cli1": CopartitionedAssignment(topics=_topics, actives={0, 1, 2},
                                        standbys={3, 4, 7}),
        "cli2": CopartitionedAssignment(topics=_topics, actives={3, 4, 5},
                                        standbys={0, 2, 6}),
    }, 1),
])
def test_get_assignment(client_assignments, replicas):
    assignment = CopartitionedAssignor(
        _topics, client_assignments,
        _num_partitions, replicas=replicas
    ).get_assignment()
    assert False, f"Assignment: {assignment}"
