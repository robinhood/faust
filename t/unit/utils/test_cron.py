import pytz
from freezegun import freeze_time
from faust.utils.cron import secs_for_next

SECS_IN_HOUR = 60 * 60


@freeze_time('2000-01-01 00:00:00')
def test_secs_for_next():
    every_minute_cron_format = '*/1 * * * *'
    assert secs_for_next(every_minute_cron_format) == 60

    every_8pm_cron_format = '0 20 * * *'
    assert secs_for_next(every_8pm_cron_format) == 20 * SECS_IN_HOUR

    every_4th_july_1pm_cron_format = '0 13 4 7 *'
    days_until_4th_july = 31 + 28 + 31 + 30 + 31 + 30 + 4
    secs_until_4th_july = SECS_IN_HOUR * 24 * days_until_4th_july
    secs_until_1_pm = 13 * SECS_IN_HOUR
    total_secs = secs_until_4th_july + secs_until_1_pm
    assert secs_for_next(every_4th_july_1pm_cron_format) == total_secs


@freeze_time('2000-01-01 00:00:00')
def test_secs_for_next_with_tz():
    pacific = pytz.timezone('US/Pacific')

    every_8pm_cron_format = '0 20 * * *'
    # In Pacific time it's 16:00 so only 4 hours until 8:00pm
    assert secs_for_next(every_8pm_cron_format, tz=pacific) == 4 * SECS_IN_HOUR
