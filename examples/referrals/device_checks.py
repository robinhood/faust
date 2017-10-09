#!/usr/bin/env python
import faust
from .models import Event, ReferralUserDevice, StockJournalUpdate


app = faust.App(
    'referrals-device-check',
    url='kafka://localhost:9092',
    default_partitions=4,
)
journal_updates_topic = app.topic('stock_journal_updates',
                                  value_type=StockJournalUpdate)
events_topic = app.topic('events_elk', value_type=Event)

user_stock_grants = app.Table('user_stock_grants', default=list)
user_devices = app.Table('user_devices', default=list)
user_usernames = app.Table('user_usernames')


@app.agent(journal_updates_topic)
async def manage_user_grants(journal_updates):
    # Sharded by user_uuid
    async for update in journal_updates.group_by(StockJournalUpdate.user_id):
        if update.state == 'unclaimed':
            user_grants = user_stock_grants[update.user_id]
            if update.id not in user_grants:
                user_grants.append(update.id)
                user_stock_grants[update.user_id] = user_grants
                await _maybe_send_user_device(update.user_id)


@app.agent(events_topic)
async def manage_user_devices(events):
    # Sharded by user_uuid
    async for event in events.group_by(lambda e: e.user.secret,
                                       name='events-by-user'):
        if not _is_valid_event(event):
            continue
        user_uuid = event.user.secret
        username = event.user.username
        device = event.device.device_id
        devices = user_devices[user_uuid]
        if device not in devices:
            devices.append(device)
            user_devices[user_uuid] = devices
            user_usernames[user_uuid] = username
            await _maybe_send_user_device(user_uuid)


def _is_valid_event(event: Event):
    return (
        event.device.device_id is not None and
        event.user.username is not None and
        event.user.secret is not None
    )


# We repartition by device for users that have claimed a stock grant
async def _maybe_send_user_device(user_uuid):
    if _should_send_user_device(user_uuid):
        for device in user_devices[user_uuid]:
            username = user_usernames[user_uuid]
            await alert_device_reuse.send(
                key=device,
                value=ReferralUserDevice(user=username, device=device),
            )


def _should_send_user_device(user_uuid):
    return (
        user_uuid in user_stock_grants and
        user_uuid in user_devices and
        user_uuid in user_usernames
    )


# We keep a list of users with referral claims that were
# accessed using the same device
device_referral_users = app.Table('device_referral_users', default=list)


@app.agent()
async def alert_device_reuse(referral_user_devices):
    # Sharded by user device
    async for user_device in referral_user_devices:
        device_users = device_referral_users[user_device.device]
        if user_device.user not in device_users:
            device_users.append(user_device.user)
            device_referral_users[user_device.device] = device_users
            if len(device_users) > 1:
                pass  # TODO Send Slack alert?

if __name__ == '__main__':
    app.main()
