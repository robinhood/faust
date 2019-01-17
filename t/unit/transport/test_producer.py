import pytest
from faust.transport.producer import Producer, TransactionProducer
from faust.types import TP
from mode import label


class ProducerTests:

    @pytest.mark.asyncio
    async def test_send(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.send('topic', 'key', 'value', 1)

    @pytest.mark.asyncio
    async def test_flush(self, *, producer):
        await producer.flush()

    @pytest.mark.asyncio
    async def test_send_and_wait(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.send_and_wait('topic', 'key', 'value', 1)

    @pytest.mark.asyncio
    async def test_create_topic(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.create_topic('topic', 1, 1)

    def test_key_partition(self, *, producer):
        with pytest.raises(NotImplementedError):
            producer.key_partition('topic', 'key')


class test_Producer(ProducerTests):

    @pytest.fixture
    def producer(self, *, app):
        return Producer(app.transport)


class test_TransactionProducer(ProducerTests):

    @pytest.fixture
    def producer(self, *, app):
        return TransactionProducer(app.transport, partition=1)

    @pytest.mark.asyncio
    async def test_commit(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.commit(
                offsets={TP('foo', 1): 130},
                group_id='group-id',
                start_new_transaction=True,
            )

    def test_transaction_id(self, *, producer):
        assert producer.transaction_id == f'{producer.app.conf.id}-1'

    def test_label(self, *, producer):
        assert label(producer) == 'TransactionProducer-1'
