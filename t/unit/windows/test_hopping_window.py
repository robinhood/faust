from faust.windows import HoppingWindow


class test_HoppingWindow:

    def test_has_ranges_including_the_value(self):
        size = 10
        step = 5
        value = 6

        window = HoppingWindow(size, step)
        window_ranges = window.ranges(value)
        assert len(window_ranges) == 2
        for range in window_ranges:
            assert range.start <= value
            assert range.end > value
