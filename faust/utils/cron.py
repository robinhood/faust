"""Crontab Utilities."""
from datetime import datetime, tzinfo
import time
from croniter.croniter import croniter


def secs_for_next(cron_format: str, tz: tzinfo = None) -> float:
    """Return seconds until next execution given crontab style format."""
    now_ts = time.time()
    # If we have a tz object we'll make now timezone aware, and
    # if not will set now to be the current timestamp (tz
    # unaware)
    # If we have tz, now will be a datetime, if not an integer
    now = tz and datetime.now(tz) or now_ts
    cron_it = croniter(cron_format, start_time=now)
    return cron_it.get_next(float) - now_ts
