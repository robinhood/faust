from datetime import timedelta, timezone
from faust.livecheck.models import State


class test_State:

    def test_is_ok(self):
        assert State.INIT.is_ok()
        assert State.PASS.is_ok()
        assert State.SKIP.is_ok()
        assert not State.FAIL.is_ok()
        assert not State.ERROR.is_ok()
        assert not State.TIMEOUT.is_ok()
        assert not State.STALL.is_ok()


class test_TestExecution:

    def test_ident(self, *, execution):
        assert execution.ident

    def test_short_ident(self, *, execution):
        assert execution.shortident

    def test_now(self, *, execution):
        assert execution._now()
        assert execution._now().tzinfo is timezone.utc

    def test_human_date__issued_today(self, *, execution):
        execution.was_issued_today = True
        assert execution.human_date

    def test_human_date__issued_before_today(self, *, execution):
        execution.was_issued_today = False
        assert execution.human_date

    def test_was_issued_today(self, *, execution):
        execution.timestamp = execution._now()
        assert execution.was_issued_today

    def test_was_issued_today__not(self, *, execution):
        execution.timestamp = execution._now() - timedelta(days=3)
        assert not execution.was_issued_today

    def test_is_expired(self, *, execution):
        execution.expires = execution._now() - timedelta(hours=1)
        assert execution.is_expired

    def test_is_expired__not(self, *, execution):
        execution.expires = execution._now() + timedelta(hours=1)
        assert not execution.is_expired
