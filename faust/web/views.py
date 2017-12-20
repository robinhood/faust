"""Class-based views."""
from typing import Any, Awaitable, Callable, Mapping, Type, cast
import jinja2
from .base import Request, Web
from ..types import AppT
from ..utils.objects import cached_property

__all__ = ['View', 'Site']


class View:
    """View (HTTP endpoint)."""

    package: str = None
    methods: Mapping[str, Callable[[Web, Request], Awaitable]]

    def __init__(self, app: AppT, web: Web) -> None:
        self.app = app
        self.web = web
        self.methods = {
            'get': self.get,
            'post': self.post,
            'patch': self.patch,
            'delete': self.delete,
            'put': self.put,
        }

    async def dispatch(self, request: Any) -> None:
        return await self.methods[request.method.lower()](
            self.web,
            cast(Request, request))

    async def get(self, web: Web, request: Request) -> Any:
        ...

    async def post(self, web: Web, request: Request) -> Any:
        ...

    async def put(self, web: Web, request: Request) -> Any:
        ...

    async def patch(self, web: Web, request: Request) -> Any:
        ...

    async def delete(self, web: Web, request: Request) -> Any:
        ...

    def render(self, template_name: str, **context: Any) -> str:
        template = self.env.get_template(template_name)
        return template.render(**context)

    @cached_property
    def env(self) -> jinja2.Environment:
        return jinja2.Environment(
            loader=jinja2.PackageLoader(self.package),
            autoescape=True,
        )


class Site:
    """Collection of HTTP endpoints (views)."""

    views: Mapping[str, Type[View]]

    def __init__(self, app: AppT) -> None:
        self.app = app

    def enable(self, web: Web, *, prefix: str = '') -> None:
        for pattern, view in self.views.items():
            web.route(prefix + pattern, view(self.app, web).dispatch)
