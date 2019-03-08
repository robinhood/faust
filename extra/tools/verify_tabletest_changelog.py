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
expected = 0
prev = None
for i, line in enumerate(fileinput.input()):
    expected += i
    found = int(line)
    if found != expected:
        print(f'At line {i} found {found} expected {expected}')
    if found == 49995000:
        print('DONE!!!!!')
