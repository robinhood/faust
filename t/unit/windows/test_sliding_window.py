from faust.windows import SlidingWindow


class test_SlidingWindow:

    def test_constructor(self):
        x = SlidingWindow(10.1, 20.2, expires=30.3)
        assert x.before == 10.1
        assert x.after == 20.2
        assert x.expires == 30.3

    def test_has_ranges_including_the_value(self):
        size = 10
        step = 5
        timestamp = 6

        window = SlidingWindow(size, step, expires=30)

        window_ranges = window.ranges(timestamp)
        assert len(window_ranges) == 1
        for range in window_ranges:
            assert range[0] <= timestamp
            assert range[1] > timestamp

    def test_current_range_is_latest_range(self):
        size = 57
        step = 23
        timestamp = 456

        window = SlidingWindow(size, step, expires=30)
        ranges = window.ranges(timestamp)
        current_range = window.current(timestamp)

        assert current_range == ranges[-1]

    def test_earliest_range_is_first_range(self):
        size = 100
        step = 15
        timestamp = 3223

        window = SlidingWindow(size, step, expires=30)
        ranges = window.ranges(timestamp)
        earliest_range = window.earliest(timestamp)

        assert earliest_range == ranges[0]

    def test_non_stale_timestamp(self):
        size = 10
        step = 5
        expires = 20

        now_timestamp = 60

        window = SlidingWindow(size, step, expires=expires)
        for time in range(now_timestamp - expires + 1, now_timestamp):
            assert window.stale(time, now_timestamp) is False

    def test_delta(self):
        size = 10
        step = 5
        expires = 20

        now_timestamp = 60
        window = SlidingWindow(size, step, expires=expires)
        assert window.delta(now_timestamp, 30) == (20, 35)

    def test_stale_timestamp(self):
        size = 10
        step = 5
        expires = 20
        now_timestamp = 60

        window = SlidingWindow(size, step, expires=expires)
        for time in range(0, now_timestamp - expires):
            print(f'TIME: {time} NOW TIMESTAMP: {now_timestamp}')
            assert window.stale(time, now_timestamp) is True
