__all__ = ['MaxRestartsExceeded']


class MaxRestartsExceeded(Exception):
    'Supervisor found restarting service too frequently.'
