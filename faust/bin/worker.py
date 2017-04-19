import click
from typing import Any, Mapping


@click.command()
@click.option('--quiet/--no-quiet', '-q', default=False)
@click.option('--debug/--no-debug', default=False)
@click.option('--logfile', '-f', default=None)
@click.option('--loglevel', '-l', default='WARN')
def worker(**kwargs: Any) -> Mapping:
    return kwargs
