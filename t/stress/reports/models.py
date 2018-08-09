import faust

__all__ = ['Error', 'Status']


class Error(faust.Record):

    #: Message (the actual formatted log message).
    message: str

    #: Format (sent to logging, e.g. 'the %s for %s did %r'
    format: str   # noqa

    hostname: str = None

    #: Traceback (if any)
    traceback: str = None

    #: Name of the origin logger.
    logger: str = None

    #: Path to the file logging this.
    filename: str = None

    #: Name of module logging this.
    module: str = None

    #: Line number
    lineno: int = None

    #: Logging severity
    severity: str = 'ERROR'

    timestamp: float = None

    app_id: str = None


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
