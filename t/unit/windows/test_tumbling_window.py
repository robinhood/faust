from faust.windows import TumblingWindow


class test_TumblingWindow:

    def test_tumbling_window_has_just_one_range(self):
        tumbling = TumblingWindow(10)
        assert len(tumbling.ranges(0)) == 1
        assert len(tumbling.ranges(5)) == 1
        assert len(tumbling.ranges(10)) == 1

    def test_end_range_in_tumbling_window_is_within_range(self):
        tumbling = TumblingWindow(10)

        # tumbling windows only have one range
        base_range = tumbling.ranges(0)[0]
        base_range_end = base_range[1]

        compare_range = tumbling.ranges(base_range_end)[0]

        assert base_range[0] == compare_range[0]
        assert base_range[1] == compare_range[1]

    def test_earliest_and_current_range_are_the_same(self):
        size = 57
        timestamp = 456
        window = TumblingWindow(size)

        assert window.current(timestamp) == window.earliest(timestamp)
