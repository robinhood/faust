from kafka.client_async import KafkaClient
from kafka.errors import NoBrokersAvailable
from typing import MutableMapping
from .protocol.admin import CreateTopicsRequest
from ...utils.logging import get_logger


logger = get_logger(__name__)


class FaustKafkaClient(KafkaClient):

    _TOPIC_CREATION_TIMEOUT = 60000  # ms

    def create_topic(self, topic: str,
                     partitions: int,
                     replication_factor: int,
                     *,
                     configs: MutableMapping[str, str]=None,
                     timeout: int=None) -> None:
        '''
        Asynchronously calls the client's send method. Must call client.poll()
        to handle success/failure. However, topic creation request is sent
        regardless of calling client.poll()

        :param topic:
        :param partitions:
        :param replication_factor:
        :param configs:
        :param timeout:
        :return:
        '''
        timeout = self._TOPIC_CREATION_TIMEOUT if timeout is None else timeout
        node_id = self.least_loaded_node()
        if node_id is None:
            raise NoBrokersAvailable()

        request = CreateTopicsRequest[1](
            [(
                topic,
                partitions,
                replication_factor,
                [],
                list((configs or {}).items()),
            )],
            timeout,
            False
        )
        future = self.send(node_id, request)
        self.poll(timeout)
        assert future.is_done
        if future.succeeded():
            response = future.value
            assert len(response.topic_error_codes) == 1
            _, err_code, err_msg = response.topic_error_codes[0]
            if err_code != 0:
                raise Exception(f'Error ({err_msg}) while creating '
                                f'topic: {topic}')
        else:
            raise future.exception

    def create_changelog_topic(self, topic: str, partitions: int,
                               replication_factor: int,
                               configs: MutableMapping[str, str]=None) -> None:
        configs = configs or {}
        # TODO: Update topic retention policy as per windowed table expiry
        configs.update({
            'cleanup.policy': 'compact',
        })
        self.create_topic(topic, partitions, replication_factor,
                          configs=configs)
