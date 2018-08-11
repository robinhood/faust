"""Partition assignor."""
import zlib
from collections import defaultdict
from typing import Iterable, List, MutableMapping, Sequence, Set, cast

from rhkafka.cluster import ClusterMetadata
from rhkafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from rhkafka.coordinator.protocol import (
    ConsumerProtocolMemberAssignment,
    ConsumerProtocolMemberMetadata,
)
from mode import get_logger
from yarl import URL

from faust.types.app import AppT
from faust.types.assignor import (
    HostToPartitionMap,
    PartitionAssignorT,
    TopicToPartitionMap,
)
from faust.types.tables import TableManagerT
from faust.types.tuples import TP

from .client_assignment import ClientAssignment, ClientMetadata
from .cluster_assignment import ClusterAssignment
from .copartitioned_assignor import CopartitionedAssignor

__all__ = [
    'MemberAssignmentMapping',
    'MemberMetadataMapping',
    'MemberSubscriptionMapping',
    'ClientMetadataMapping',
    'ClientAssignmentMapping',
    'CopartitionedGroups',
    'PartitionAssignor',
]

MemberAssignmentMapping = MutableMapping[str, ConsumerProtocolMemberAssignment]
MemberMetadataMapping = MutableMapping[str, ConsumerProtocolMemberMetadata]
MemberSubscriptionMapping = MutableMapping[str, List[str]]
ClientMetadataMapping = MutableMapping[str, ClientMetadata]
ClientAssignmentMapping = MutableMapping[str, ClientAssignment]
CopartitionedGroups = MutableMapping[int, Iterable[Set[str]]]

logger = get_logger(__name__)


class PartitionAssignor(AbstractPartitionAssignor, PartitionAssignorT):
    """PartitionAssignor handles internal topic creation.

    Further, this assignor needs to be sticky and potentially redundant

    Notes:
        Interface copied from
        https://github.com/dpkp/kafka-python/blob/master/
        kafka/coordinator/assignors/abstract.py
    """

    _metadata: ClientMetadata
    _assignment: ClientAssignment
    _table_manager: TableManagerT
    _member_urls: MutableMapping[str, str]
    _changelog_distribution: HostToPartitionMap
    _active_tps: Set[TP]
    _standby_tps: Set[TP]
    _tps_url: MutableMapping[TP, str]

    def __init__(self, app: AppT, replicas: int = 0) -> None:
        AbstractPartitionAssignor.__init__(self)
        self.app = app
        self._table_manager = self.app.tables
        self._assignment = ClientAssignment(actives={}, standbys={})
        self._changelog_distribution = {}
        self.replicas = replicas
        self._member_urls = {}
        self._tps_url = {}
        self._active_tps = set()
        self._standby_tps = set()

    @property
    def changelog_distribution(self) -> HostToPartitionMap:
        return self._changelog_distribution

    @changelog_distribution.setter
    def changelog_distribution(self, value: HostToPartitionMap) -> None:
        self._changelog_distribution = value
        self._tps_url = {
            TP(topic, partition): url
            for url, tps in self._changelog_distribution.items()
            for topic, partitions in tps.items() for partition in partitions
        }

    @property
    def _metadata(self) -> ClientMetadata:
        return ClientMetadata(
            assignment=self._assignment,
            url=str(self._url),
            changelog_distribution=self.changelog_distribution,
        )

    @property
    def _url(self) -> URL:
        return self.app.conf.canonical_url

    def on_assignment(
            self, assignment: ConsumerProtocolMemberMetadata) -> None:
        metadata = cast(ClientMetadata,
                        ClientMetadata.loads(
                            self._decompress(assignment.user_data)))
        self._assignment = metadata.assignment
        self._active_tps = self._assignment.active_tps
        self._standby_tps = self._assignment.standby_tps
        self.changelog_distribution = metadata.changelog_distribution
        a = sorted(assignment.assignment)
        b = sorted(
            self._assignment.kafka_protocol_assignment(self._table_manager))
        assert a == b, f'{a!r} != {b!r}'
        assert metadata.url == str(self._url)

    def metadata(self, topics: Set[str]) -> ConsumerProtocolMemberMetadata:
        return ConsumerProtocolMemberMetadata(self.version, list(topics),
                                              self._metadata.dumps())

    @classmethod
    def _group_co_subscribed(cls, topics: Set[str],
                             subscriptions: MemberSubscriptionMapping,
                             ) -> Iterable[Set[str]]:
        topic_subscriptions: MutableMapping[str, Set[str]] = defaultdict(set)
        for client, subscription in subscriptions.items():
            for topic in subscription:
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
            subscriptions: MemberSubscriptionMapping) -> CopartitionedGroups:
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
            num_partitions: cls._group_co_subscribed(topics, subscriptions)
            for num_partitions, topics in topics_by_partitions.items()
        }
        return copart_grouped

    @classmethod
    def _get_client_metadata(
            cls, metadata: ConsumerProtocolMemberMetadata) -> ClientMetadata:
        client_metadata = ClientMetadata.loads(metadata.user_data)
        return cast(ClientMetadata, client_metadata)

    def _update_member_urls(self,
                            clients_metadata: ClientMetadataMapping) -> None:
        self._member_urls = {
            member_id: client_metadata.url
            for member_id, client_metadata in clients_metadata.items()
        }

    def assign(self, cluster: ClusterMetadata,
               member_metadata: MemberMetadataMapping,
               ) -> MemberAssignmentMapping:
        cluster_assgn = ClusterAssignment()

        clients_metadata = {
            member_id: self._get_client_metadata(metadata)
            for member_id, metadata in member_metadata.items()
        }

        subscriptions = {
            member_id: cast(List[str], metadata.subscription)
            for member_id, metadata in member_metadata.items()
        }

        for member_id in member_metadata:
            cluster_assgn.add_client(member_id, subscriptions[member_id],
                                     clients_metadata[member_id])
        topics = cluster_assgn.topics()

        copartitioned_groups = self._get_copartitioned_groups(
            topics, cluster, subscriptions)

        self._update_member_urls(clients_metadata)

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
                    replicas=self.replicas,
                )
                # Update client assignments for copartitioned group
                for client, copart_assn in assignor.get_assignment().items():
                    assignments[client].add_copartitioned_assignment(
                        copart_assn)

        changelog_distribution = self._get_changelog_distribution(assignments)
        res = self._protocol_assignments(assignments, changelog_distribution)
        return res

    def _protocol_assignments(
            self,
            assignments: ClientAssignmentMapping,
            cl_distribution: HostToPartitionMap) -> MemberAssignmentMapping:
        return {
            client: ConsumerProtocolMemberAssignment(
                self.version,
                sorted(
                    assignment.kafka_protocol_assignment(self._table_manager)),
                self._compress(
                    ClientMetadata(
                        assignment=assignment,
                        url=self._member_urls[client],
                        changelog_distribution=cl_distribution,
                    ).dumps(),
                ),
            )
            for client, assignment in assignments.items()
        }

    @classmethod
    def _compress(cls, raw: bytes) -> bytes:
        return zlib.compress(raw)

    @classmethod
    def _decompress(cls, compressed: bytes) -> bytes:
        return zlib.decompress(compressed)

    @classmethod
    def _topics_filtered(cls, assignment: TopicToPartitionMap,
                         topics: Set[str]) -> TopicToPartitionMap:
        return {
            topic: partitions
            for topic, partitions in assignment.items() if topic in topics
        }

    def _get_changelog_distribution(
            self, assignments: ClientAssignmentMapping) -> HostToPartitionMap:
        topics = self._table_manager.changelog_topics
        return {
            self._member_urls[client]: self._topics_filtered(
                assignment.actives, topics)
            for client, assignment in assignments.items()
        }

    @property
    def name(self) -> str:
        return 'faust'

    @property
    def version(self) -> int:
        return 3

    def assigned_standbys(self) -> Set[TP]:
        return {
            TP(topic, partition)
            for topic, partitions in self._assignment.standbys.items()
            for partition in partitions
        }

    def assigned_actives(self) -> Set[TP]:
        return {
            TP(topic, partition)
            for topic, partitions in self._assignment.actives.items()
            for partition in partitions
        }

    def table_metadata(self, topic: str) -> HostToPartitionMap:
        return {
            host: self._topics_filtered(assignment, {topic})
            for host, assignment in self.changelog_distribution.items()
        }

    def tables_metadata(self) -> HostToPartitionMap:
        return self.changelog_distribution

    def key_store(self, topic: str, key: bytes) -> URL:
        return URL(self._tps_url[self.app.producer.key_partition(topic, key)])

    def is_active(self, tp: TP) -> bool:
        return tp in self._active_tps

    def is_standby(self, tp: TP) -> bool:
        return tp in self._standby_tps
