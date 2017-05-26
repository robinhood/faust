from asyncio import AbstractEventLoop
from datetime import timedelta
from typing import MutableMapping
from aiokafka.client import AIOKafkaClient
from .protocol.admin import CreateTopicsRequest


__all__ = [
    'create_topic',
    'create_changelog_topic',
]


async def create_topic(loop: AbstractEventLoop,
                       topic: str,
                       partitions: int,
                       replication: int,
                       *,
                       configs: MutableMapping[str, str] = None,
                       timeout: int = 10000) -> None:
    '''
    Creates topic. Throws exception if unable to create topic.

    :param loop:
    :param topic:
    :param partitions:
    :param replication:
    :param configs:
    :param timeout: in milliseconds
    :return:
    '''
    client = AIOKafkaClient(loop=loop, bootstrap_servers='127.0.0.1:9092')
    await client.bootstrap()
    node_id = next(broker.nodeId for broker in client.cluster.brokers())

    request = CreateTopicsRequest[1]([
        (topic, partitions, replication, [], list((configs or {}).items()))],
        timeout,
        False
    )
    response = await client.send(node_id, request)
    assert len(response.topic_error_codes) == 1, "Only 1 topic requested"
    _, err_code, err_msg = response.topic_error_codes[0]
    if err_code != 0:
        raise Exception(f'Error: <{err_msg}> Creating topic: {topic}')


async def create_changelog_topic(loop: AbstractEventLoop,
                                 topic: str,
                                 partitions: int,
                                 replication: int,
                                 *,
                                 configs: MutableMapping[str, str] = None,
                                 retention: timedelta = None,
                                 timeout: int = 10000) -> None:
    configs = configs or {}
    configs['cleanup.policy'] = 'compact'
    if retention is not None:
        configs['cleanup.policy'] += ',delete'
        configs['retention.ms'] = int(retention.total_seconds() * 1000)
    await create_topic(loop, topic, partitions, replication,
                       configs=configs, timeout=timeout)
