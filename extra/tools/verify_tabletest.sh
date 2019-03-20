#!/bin/bash

_INFO=$(
    python -c'
from examples.tabletest import EXPECTED_SUM, ITERATIONS, counts, app
print("{0}:{1}:{2}:{3}".format(
    counts.changelog_topic.get_topic_name(),
    counts.changelog_topic.partitions or app.conf.topic_partitions,
    ITERATIONS,
    EXPECTED_SUM,
))
')

CHANGELOG_TOPIC=$(echo $_INFO | cut -d: -f1)
CHANGELOG_PARTITIONS=$(echo $_INFO | cut -d: -f2)
ITERATIONS=$(echo $_INFO | cut -d: -f3)
export EXPECTED_SUM=$(echo $_INFO | cut -d: -f4)

if [ -z "$CHANGELOG_TOPIC" ]; then
    echo "Cannot find changelog topic name :-("
    exit 1
fi

for i in $(seq 0 $(($CHANGELOG_PARTITIONS - 1))); do
    echo "Verifying changelog partition $i for $CHANGELOG_TOPIC..."
    kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --max-messages=$ITERATIONS \
        --topic "$CHANGELOG_TOPIC" \
        --partition=$i \
        --from-beginning |
            python extra/tools/verify_tabletest_changelog.py
    if [[ $? != 0 ]]; then
        echo "exiting"
        exit $?
    fi
done
echo "ALL GOOD!"
