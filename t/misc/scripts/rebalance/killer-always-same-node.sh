#!/bin/bash

# Usage:
# $ ./killer.sh [min_delay [max_delay]]
#
# No arguments defaults to min_delay=1.5 max_delay=10.0
#
# $ ./killer.sh

MIN_SECS=${1:-1.5}
MAX_SECS=${2:-10.0}

while $(true); do
    python -c'
import random,time,sys
secs=random.uniform(float(sys.argv[1]), float(sys.argv[2]))
print(f"Sleeping for {secs} seconds...")
time.sleep(secs)
    ' $MIN_SECS $MAX_SECS
    ps auxwww | \
        grep 'Faust:Worker' | \
        grep /w1 | \
        grep -v 'grep' | \
        awk '{print $2}' | \
        xargs python3 -c '
import random,os,sys
pid=int(random.choice(sys.argv[1:]))
print(f"Killing {pid!r}!")
os.kill(pid, 15)
        '
done
