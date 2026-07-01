"""Dependency injection module — Depends, Context, Header, Path, DIResolver.

Public API re-exported here per the project convention that each package's
``__init__.py`` re-exports its public symbols.
"""

from rabbitkit.di.context import Context, ContextRepo, Header, Path
from rabbitkit.di.depends import Depends
from rabbitkit.di.resolver import DependencyScope, DIResolver

__all__ = [
    "Context",
    "ContextRepo",
    "DIResolver",
    "DependencyScope",
    "Depends",
    "Header",
    "Path",
]
