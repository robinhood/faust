from faust.utils.log import get_logger


def test_get_logger():
    assert get_logger(__name__)
    assert get_logger(__name__).handlers == get_logger(__name__).handlers
