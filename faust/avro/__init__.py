"""Avro serialization."""
from .serializer import MessageSerializer
from .server import RegistryClient

__all__ = ['MessageSerializer', 'RegistryClient']
