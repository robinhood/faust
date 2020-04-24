from faust.windows import SessionWindow


class test_SessionWindow:

    def test_session_window_has_just_one_range(self):
        session = SessionWindow(10)
        assert len(session.ranges(0)) == 1
        assert len(session.ranges(5)) == 1
        assert len(session.ranges(10)) == 1

    def test_end_range_in_session_window_is_within_range(self):
        session = SessionWindow(10)

        # session windows only have one range
        base_range = session.ranges(0)[0]
        base_range_end = base_range[1]

        compare_range = session.ranges(base_range_end)[0]

        assert base_range[0] == compare_range[0]
        assert base_range[1] == compare_range[1]

    def test_earliest_and_current_range_are_the_same(self):
        size = 57
        timestamp = 456
        window = SessionWindow(size)

        assert window.current(timestamp) == window.earliest(timestamp)
