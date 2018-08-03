"""
Celery-like task queue implemented using Faust.

This example comes with a background thread that
sends a task every second.

After starting Kafka locally, you can run the example:

.. sourcecode:: console

    $ python examples/task_queue.py worker -l info

You can also see stats about the worker by visiting http://localhost:6066.
"""
import random
from typing import Any, Awaitable, Callable, Mapping, MutableMapping, Sequence
import faust
from mode.utils.objects import qualname


class Request(faust.Record):
    """Describes how tasks are serialized and sent to Kafka."""

    #: Correlation ID, can be used to pass results back to caller.
    id: str

    #: Name of the task as registered in the task_registry.
    name: str

    #: Positional arguments to the task.
    arguments: Sequence

    #: Keyword arguments to the task.
    keyword_arguments: Mapping

    async def __call__(self) -> Any:
        return await self.handler(*self.arguments, **self.keyword_arguments)

    @property
    def handler(self) -> Callable[..., Awaitable]:
        return task_registry[self.name]


app = faust.App('faust-celery', broker='kafka://localhost')

task_queue_topic = app.topic('tasks', value_type=Request)

task_registry: MutableMapping[str, Callable[..., Awaitable]]
task_registry = {}


@app.agent(task_queue_topic)
async def process_task(tasks: faust.Stream[Request]) -> None:
    """A "worker" stream processor that executes tasks."""
    async for task in tasks:
        print(f'Processing task: {task!r}')
        result = await task()
        print(f'Result of {task.id} is: {result!r}')


class Task:

    def __init__(self, fun: Callable[..., Awaitable],
                 *,
                 name: str = None) -> None:
        self.fun: Callable[..., Awaitable] = fun
        self.name = name or qualname(fun)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.fun(*args, **kwargs)

    async def delay(self, *args: Any, **kwargs: Any) -> Any:
        return await self.apply_async(args, kwargs)

    async def apply_async(self,
                          args: Sequence,
                          kwargs: Mapping,
                          id: str = None,
                          **options: Any) -> None:
        id = id or faust.uuid()
        return await process_task.send(value=Request(
            id=id,
            name=self.name,
            arguments=args,
            keyword_arguments=kwargs,
        ))


def task(fun: Callable) -> Task:
    # Task decorator
    task = Task(fun)
    task_registry[task.name] = task
    return task


@task
async def add(x: int, y: int) -> int:
    return x + y


@app.timer(1.0)
async def _send_tasks() -> None:
    await add.delay(random.randint(0, 100), random.randint(0, 100))


if __name__ == '__main__':
    app.main()
