#!/usr/bin/env python
import fileinput

# Usage:
#
# kafka-console-consumer --bootstrap-server localhost:9092 \
#       --topic tabletest-v2-counts-changelog \
#       --partition=2 --from-beginning | \
#    python extra/tools/verify_tabletest_changelog.py

expected = 0
prev = None
for i, line in enumerate(fileinput.input()):
    expected += i
    found = int(line)
    if found != expected:
        print(f'At line {i} found {found} expected {expected}')
    if found == 49995000:
        print('DONE!!!!!')
