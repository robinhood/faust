import faust


class _Device(faust.Record):
    device_id: str = None


class _User(faust.Record):
    username: str = None
    secret: str = None


class Event(faust.Record):
    user: _User
    device: _Device


class StockJournalUpdate(faust.Record):
    state: str
    id: str
    instrument_id: str
    user_id: str
    quantity: float
    granted_at: str
    cost_basis: float


class ReferralUserDevice(faust.Record):
    user: str
    device: str


class StockGrant(faust.Record):
    id: str
    timestamp: str
    user_uuid: str
    symbol: str
    cost_basis: float
    quantity: float
