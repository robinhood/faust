import asyncio
import sys
from typing import Iterator

CONSUMER_BIN = 'kafka-console-consumer'
CONSUMER_FLAGS = '--from-beginning'
CONSUMER_TEMPLATE = '''
    kafka-console-consumer --bootstrap-server='{host}:{port}' \
    --topic='{topic}' \
    --partition='{partition}' \
    --timeout-ms=500 \
    {flags}
'''.strip()


async def kafka_console_consume(
        host: str,
        port: int,
        topic: str,
        partition: int) -> Iterator[bytes]:
    cmd = kafka_console_consumer(host, port, topic, partition)
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout, stderr = await proc.communicate()
    if stderr:
        error = stderr.decode()
        if 'TimeoutException' not in error:
            if 'Processed a total' not in error:
                print(f'Command {cmd} gave error: {error}', file=sys.stderr)
    if proc.returncode:
        print(f'Process exited with error status {proc.returncode}',
              file=sys.stderr)
    return stdout.splitlines()


def kafka_console_consumer(
        host: str, port: int, topic: str, partition: int,
        flags=CONSUMER_FLAGS) -> str:
    return CONSUMER_TEMPLATE.format(
        host=host,
        port=port,
        topic=topic,
        partition=partition,
        flags=flags,
    )


if __name__ == '__main__':
    async def main():
        print(await kafka_console_consume(
            'localhost', 9092, 'faust-integrity-commit-offsets10', 0))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
