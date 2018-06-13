#!/bin/bash
#
# Usage:
# $ ./start_worker [worker_number]
#
# Example:
# $ ./start_worker 1

ID=$1
APP="examples/advanced/isolated_partitions_crashing.py"
LEVEL="INFO"

if [ -z "$ID" ]; then
    echo "Missing argument: worker number" >&2
    exit 64
fi

DATADIR="w${ID}"
WEB=$((6066 + $ID - 1))

COMMAND="python "$APP" --datadir="$DATADIR" worker -l $LEVEL --web-port=$WEB"
while $(true); do
    echo "Starting worker with arguments:"
    echo $COMMAND
    $COMMAND
done

