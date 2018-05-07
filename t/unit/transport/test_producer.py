import pytest
from faust.transport.producer import Producer


class test_Producer:

    @pytest.fixture
    def producer(self, *, app):
        return Producer(app.transport)

    @pytest.mark.asyncio
    async def test_send(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.send('topic', 'key', 'value', 1)

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
