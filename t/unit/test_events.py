import pytest
from mode.utils.mocks import AsyncMock, Mock
from faust import Event


class test_Event:

    @pytest.fixture
    def key(self):
        return Mock(name='key')

    @pytest.fixture
    def value(self):
        return Mock(name='value')

    @pytest.fixture
    def message(self):
        return Mock(name='message')

    @pytest.fixture
    def event(self, *, app, key, value, message):
        return Event(app, key, value, message)

    @pytest.mark.asyncio
    async def test_send(self, *, event):
        callback = Mock(name='callback')
        event._send = AsyncMock(name='event._send')
        await event.send(
            channel='chan',
            key=event.key,
            value=event.value,
            partition=3,
            key_serializer='kser',
            value_serializer='vset',
            callback=callback,
        )
        event._send.assert_called_once_with(
            'chan',
            event.key,
            event.value,
            3,
            'kser',
            'vset',
            callback,
            force=False,
        )

    @pytest.mark.asyncio
    async def test_send__USE_EXISTING_KEY_VALUE(self, *, event):
        callback = Mock(name='callback')
        event._send = AsyncMock(name='event._send')
        await event.send(
            channel='chan',
            partition=3,
            key_serializer='kser',
            value_serializer='vset',
            callback=callback,
        )
        event._send.assert_called_once_with(
            'chan',
            event.key,
            event.value,
            3,
            'kser',
            'vset',
            callback,
            force=False,
        )

    @pytest.mark.asyncio
    async def test_forward(self, *, event):
        callback = Mock(name='callback')
        event._send = AsyncMock(name='event._send')
        await event.forward(
            channel='chan',
            key=event.key,
            value=event.value,
            partition=3,
            key_serializer='kser',
            value_serializer='vset',
            callback=callback,
        )
        event._send.assert_called_once_with(
            'chan',
            event.key,
            event.value,
            3,
            'kser',
            'vset',
            callback,
            force=False,
        )

    @pytest.mark.asyncio
    async def test_forward__USE_EXISTING_KEY_VALUE(self, *, event):
        callback = Mock(name='callback')
        event._send = AsyncMock(name='event._send')
        await event.forward(
            channel='chan',
            partition=3,
            key_serializer='kser',
            value_serializer='vset',
            callback=callback,
        )
        event._send.assert_called_once_with(
            'chan',
            event.message.key,
            event.message.value,
            3,
            'kser',
            'vset',
            callback,
            force=False,
        )

    def test_attach(self, *, event, app):
        callback = Mock(name='callback')
        app._attachments.put = Mock(name='_attachments.put')
        result = event._attach(
            channel='chan',
            key=b'k',
            value=b'v',
            partition=3,
            key_serializer='kser',
            value_serializer='vset',
            callback=callback,
        )
        app._attachments.put.assert_called_once_with(
            event.message,
            'chan',
            b'k',
            b'v',
            partition=3,
            key_serializer='kser',
            value_serializer='vset',
            callback=callback,
        )
        assert result is app._attachments.put()

    def test_repr(self, *, event):
        assert repr(event)

    @pytest.mark.asyncio
    async def test_AsyncContextManager(self, *, event):
        event.ack = Mock(name='ack')
        block_executed = False
        async with event:
            block_executed = True
        assert block_executed

        event.ack.assert_called_once_with()
