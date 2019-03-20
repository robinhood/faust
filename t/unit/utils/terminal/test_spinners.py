import logging
from faust.utils.terminal.spinners import Spinner, SpinnerHandler
from mode.utils.mocks import Mock, call, patch


def spinner(file=None, isatty=True):
    file = file if file is not None else Mock(name='file')
    file.isatty.return_value = isatty
    return Spinner(file)


class test_Spinner:

    def spinner(self, file=None, isatty=True):
        return spinner(file=file, isatty=isatty)

    def test_constructor(self):
        f = Mock(name='file')
        spinner = self.spinner(f)
        assert spinner.file is f
        assert spinner.width == 0
        assert spinner.count == 0
        assert not spinner.stopped

    def test_update(self):
        s = self.spinner()
        assert s.count == 0
        s.begin = Mock(name='begin')
        s.write = Mock(name='write')
        s.update()
        s.begin.assert_called_once_with()
        assert s.count == 1
        s.write.assert_called_once_with(s.sprites[0])

        for i in range(100):
            s.update()
            s.begin.assert_called_once_with()
            assert s.count == i + 2
            s.write.assert_called_with(
                s.sprites[(s.count - 1) % len(s.sprites)])
        count = s.count
        s.stop()
        assert s.stopped
        s.update()
        assert s.count == count

    def test_reset(self):
        s = self.spinner(isatty=True)
        s.reset()
        assert not s.stopped
        assert s.count == 0

    def test_write(self):
        s = self.spinner(isatty=True)
        s.write('f')
        s.file.write.assert_has_calls([
            call('f'), call(''),
        ])
        s.file.flush.assert_called_once_with()

    def test_write__notatty(self):
        s = self.spinner(isatty=False)
        s.write('f')
        s.file.write.assert_not_called()

    def test_begin(self):
        s = self.spinner()
        with patch('atexit.register') as atexit_register:
            s.begin()
            s.file.write.assert_has_calls([
                call(s.cursor_hide), call(''),
            ])
            s.file.flush.assert_called_once_with()
            atexit_register.assert_called_with(
                type(s)._finish, s.file, at_exit=True)

    def test_finish(self):
        s = self.spinner()
        s.finish()
        s.file.write.assert_has_calls([
            call(s.bell), call(''), call(s.cursor_show), call(''),
        ])
        s.file.flush.assert_called_once_with()
        assert s.stopped


def test_SpinnerHandler():
    s = Mock(name='spinner', autospec=Spinner)
    s.stopped = False
    handler = SpinnerHandler(spinner=s)
    assert handler.spinner is s
    handler.emit(Mock(name='logrecord', autospec=logging.LogRecord))
    handler.spinner.update.assert_called_once_with()


def test_SpinnerHandler__no_spinner():
    SpinnerHandler(spinner=None).emit(
        Mock(
            name='logrecord',
            autospec=logging.LogRecord,
        ),
    )
