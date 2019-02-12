from faust.web import exceptions


def test_WebError():
    exc = exceptions.WebError(detail='detail', code=400)
    assert exc.detail == 'detail'
    assert exc.code == 400


def test_WebError_defaults():
    exc = exceptions.WebError()
    assert exc.detail == exceptions.WebError.detail
    assert exc.code is None
