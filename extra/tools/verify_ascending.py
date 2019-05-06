#!/usr/bin/env python
"""
Usage:

.. sourcecode:: console

    $ kafka-console-consumer --bootstrap-server localhost:9092 \
       --topic example-source-topic2 \
       --from-beginning | \
        python extra/tools/verify_ascending.py

"""


import fileinput
import os
import sys

STOP = os.environ.get('STOP', 50000)

expected = 0
prev = None
for i, line in enumerate(fileinput.input()):
    expected = i + 1
    found = int(line.strip().strip('"'))
    if found != expected:
        print(f'At line {i} found {found} expected {expected}')
        sys.exit(445)
    if found >= STOP:
        print('Ok')
        sys.exit(0)
