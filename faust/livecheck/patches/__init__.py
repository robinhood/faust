from . import aiohttp

__all__ = ['aiohttp', 'patch_all']


def patch_all() -> None:
    aiohttp.patch_all()
