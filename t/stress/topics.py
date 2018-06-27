from .app import app
from .models import Withdrawal

withdrawals = app.topic('f-stress-withdrawals', value_type=Withdrawal)
