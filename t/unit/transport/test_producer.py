import pytest
from mode.utils.mocks import AsyncMock, Mock, call
from faust.transport.producer import Producer, ProducerBuffer


class test_ProducerBuffer:

    @pytest.fixture()
    def buf(self):
        return ProducerBuffer()

    def test_put(self, *, buf):
        fut = Mock(name='future_message')
        buf.pending = Mock(name='pending')
        buf.put(fut)

        buf.pending.put_nowait.assert_called_once_with(fut)

    @pytest.mark.asyncio
    async def test_on_stop(self, *, buf):
        buf.flush = AsyncMock(name='flush')
        await buf.on_stop()
        buf.flush.coro.assert_called_once_with()

    @pytest.mark.asyncio
    async def test__send_pending(self, *, buf):
        fut = Mock(name='future_message')
        fut.message.channel.publish_message = AsyncMock()
        await buf._send_pending(fut)
        fut.message.channel.publish_message.coro.assert_called_once_with(
            fut, wait=False,
        )

    @pytest.mark.asyncio
    async def test__handle_pending(self, *, buf):
        buf.pending = Mock(get=AsyncMock())
        buf._send_pending = AsyncMock()

        async def on_send(fut):
            if buf._send_pending.call_count >= 3:
                buf._stopped.set()

        buf._send_pending.side_effect = on_send

        await buf._handle_pending(buf)

        buf._send_pending.assert_has_calls([
            call(buf.pending.get.coro.return_value),
            call(buf.pending.get.coro.return_value),
        ])

    @pytest.mark.asyncio
    async def test_wait_until_ebb(self, *, buf):
        buf.max_messages = 10
        buf._send_pending = AsyncMock()
        self._put(buf, range(20))
        assert buf.size == 20

        await buf.wait_until_ebb()
        assert list(buf.pending._queue) == list(range(10, 20))
        assert buf.size == 10

        await buf.wait_until_ebb()
        assert list(buf.pending._queue) == list(range(10, 20))
        assert buf.size == 10

    @pytest.mark.asyncio
    async def test_flush(self, *, buf):
        buf._send_pending = AsyncMock()
        assert not buf.size
        await buf.flush()

        self._put(buf, range(10))
        assert buf.size == 10

        await buf.flush()
        assert not buf.size

    def _put(self, buf, items):
        for item in items:
            buf.pending.put_nowait(item)

    @pytest.mark.asyncio
    async def test_flush_atmost(self, *, buf):
        buf._send_pending = AsyncMock()
        assert await buf.flush_atmost(10) == 0

        self._put(buf, range(3))
        assert buf.size == 3
        assert await buf.flush_atmost(10) == 3

        self._put(buf, range(10))
        assert buf.size == 10
        assert (await buf.flush_atmost(4)) == 4
        assert buf.size == 6

        assert (await buf.flush_atmost(6)) == 6
        assert not buf.size


class ProducerTests:

    @pytest.mark.asyncio
    async def test_send(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.send('topic', 'key', 'value', 1, None, {})

    def test_send_soon(self, *, producer):
        producer.buffer = Mock(name='buffer')
        fut = Mock(name='fut')
        producer.send_soon(fut)
        producer.buffer.put.assert_called_once_with(fut)

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
