from faust.windows import HoppingWindow


class test_HoppingWindow:

    def test_has_ranges_including_the_value(self):
        size = 10
        step = 5
        timestamp = 6

        window = HoppingWindow(size, step)

        window_ranges = window.ranges(timestamp)
        assert len(window_ranges) == 2
        for range in window_ranges:
            assert range[0] <= timestamp
            assert range[1] > timestamp

    def test_current_range_is_latest_range(self):
        size = 57
        step = 23
        timestamp = 456

        window = HoppingWindow(size, step)
        ranges = window.ranges(timestamp)
        current_range = window.current(timestamp)

        assert current_range == ranges[-1]

    def test_earliest_range_is_first_range(self):
        size = 100
        step = 15
        timestamp = 3223

        window = HoppingWindow(size, step)
        ranges = window.ranges(timestamp)
        earliest_range = window.earliest(timestamp)

        assert earliest_range == ranges[0]

    def test_non_stale_timestamp(self):
        size = 10
        step = 5
        expires = 20

        now_timestamp = 60

        window = HoppingWindow(size, step, expires=expires)
        for time in range(now_timestamp - expires + 1, now_timestamp):
            assert window.stale(time, now_timestamp) is False

    def test_stale_timestamp(self):
        size = 10
        step = 5
        expires = 20
        now_timestamp = 60

        window = HoppingWindow(size, step, expires=expires)
        for time in range(0, now_timestamp - expires):
            print(f'TIME: {time} NOW TIMESTAMP: {now_timestamp}')
            assert window.stale(time, now_timestamp) is True
