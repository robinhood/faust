"""Platform/OS utilities."""
import platform
import subprocess
from typing import Optional


def max_open_files() -> Optional[int]:
    """Return max number of open files, or :const:`None`."""
    try:
        from resource import RLIM_INFINITY, RLIMIT_NOFILE, getrlimit
    except ImportError:
        return None
    else:
        _, hard_limit = getrlimit(RLIMIT_NOFILE)
        if hard_limit == RLIM_INFINITY:
            # macOS bash always returns infinity, even though there
            # is an actual system limit.
            if platform.system() == 'Darwin':
                output = subprocess.check_output([
                    'sysctl', '-q', 'kern.maxfilesperproc',
                ])
                # $ sysctl -q kern.maxfilesperproc
                # kern.maxfilesperproc: 24576
                _, _, svalue = output.decode().partition(':')
                return int(svalue.strip())
            return None
        return hard_limit
