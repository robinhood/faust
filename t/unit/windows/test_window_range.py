from faust.windows import WindowRange
from faust.utils.json import dumps


class test_WindowRange:

    def test_window_range_json_dump(self):
        serialized = dumps(WindowRange(1, 2))
        assert serialized == '[1, 2]'
