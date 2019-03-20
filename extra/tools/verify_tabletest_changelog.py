#!/usr/bin/env python
"""
Usage:

.. sourcecode:: console

    $ kafka-console-consumer --bootstrap-server localhost:9092 \
       --topic tabletest-counts-changelog \
       --partition=0 --from-beginning | \
        python extra/tools/verify_tabletest_changelog.py

"""


import fileinput
import os
import sys

EXPECTED_SUM = int(os.environ.get('EXPECTED_SUM', 49995000))

expected = 0
prev = None
for i, line in enumerate(fileinput.input()):
    expected += i
    found = int(line)
    if found != expected:
        print(f'At line {i} found {found} expected {expected}')
        sys.exit(445)
    if found == EXPECTED_SUM:
        print('DONE!')
        sys.exit(0)
