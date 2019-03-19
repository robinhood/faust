from faust.utils import uuid


def test_uuid():
    seen = set()
    for _ in range(100):
        uid = uuid()
        assert uid not in seen
        seen.add(uid)
