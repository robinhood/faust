# -*- coding: utf-8 -*-
"""Example Django project using Faust."""
# :copyright: (c) 2017-2018, Robinhood Markets, Inc.
#             All rights reserved.
# :license:   BSD (3 Clause), see LICENSE for more details.

# -- Faust is a Python stream processing library
# mainly used with Kafka, but is generic enough to be used for general
# agent and channel based programming.

import re
from typing import NamedTuple

__version__ = '0.9.3'
__author__ = 'Robinhood Markets, Inc.'
__contact__ = 'opensource@robinhood.com'
__homepage__ = 'http://fauststream.com'
__docformat__ = 'restructuredtext'

# -eof meta-


class version_info_t(NamedTuple):
    major: int
    minor: int
    micro: int
    releaselevel: str
    serial: str


# bumpversion can only search for {current_version}
# so we have to parse the version here.
_temp = re.match(
    r'(\d+)\.(\d+).(\d+)(.+)?', __version__).groups()
VERSION = version_info = version_info_t(
    int(_temp[0]), int(_temp[1]), int(_temp[2]), _temp[3] or '', '')
del(_temp)
del(re)

__all__ = []
