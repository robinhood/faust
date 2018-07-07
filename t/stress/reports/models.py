import faust

STATE_OK = 'OK'
STATE_FAIL = 'FAIL'
STATE_SLOW = 'SLOW'
STATE_STALL = 'STALL'
STATE_UNASSIGNED = 'UNASSIGNED'
STATE_REBALANCING = 'REBALANCING'

OK_STATES = {STATE_OK, STATE_UNASSIGNED}
MAYBE_STATES = {STATE_REBALANCING}


class Status(faust.Record):

    #: The id of the app that is sending this.
    app_id: str

    #: Worker hostname
    hostname: str

    #: What test is being reported on.
    category: str

    #: What state is this test in
    state: str

    #: What color should we display this as
    color: str

    #: How many times did this fail so far.
    count: int

    #: Severity of issue
    severity: str

    @property
    def key(self):
        return (self.app_id, self.hostname, self.category)

    @property
    def details(self):
        return {
            'state': self.state,
            'count': self.count,
            'severity': self.severity,
            'color': self.color,
        }
