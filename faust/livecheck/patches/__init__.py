"""Patches - LiveCheck integration with other frameworks/libraries."""
from . import aiohttp

__all__ = ['aiohttp', 'patch_all']


def patch_all() -> None:
    """Apply all LiveCheck monkey patches."""
    aiohttp.patch_all()
