import pytest
from faust.transport.producer import Producer


class ProducerTests:

    @pytest.mark.asyncio
    async def test_send(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.send('topic', 'key', 'value', 1, None, {})

    @pytest.mark.asyncio
    async def test_flush(self, *, producer):
        await producer.flush()

    @pytest.mark.asyncio
    async def test_send_and_wait(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.send_and_wait('topic', 'key', 'value', 1, None, {})

    @pytest.mark.asyncio
    async def test_create_topic(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.create_topic('topic', 1, 1)

    def test_key_partition(self, *, producer):
        with pytest.raises(NotImplementedError):
            producer.key_partition('topic', 'key')

    @pytest.mark.asyncio
    async def test_begin_transaction(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.begin_transaction('tid')

    @pytest.mark.asyncio
    async def test_commit_transaction(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.commit_transaction('tid')

    @pytest.mark.asyncio
    async def test_abort_transaction(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.abort_transaction('tid')

    @pytest.mark.asyncio
    async def test_stop_transaction(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.stop_transaction('tid')

    @pytest.mark.asyncio
    async def test_maybe_begin_transaction(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.maybe_begin_transaction('tid')

    @pytest.mark.asyncio
    async def test_commit_transactions(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.commit_transactions({}, 'gid')

    def test_supports_headers(self, *, producer):
        assert not producer.supports_headers()


class test_Producer(ProducerTests):

    @pytest.fixture
    def producer(self, *, app):
        return Producer(app.transport)
