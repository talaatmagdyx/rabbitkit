"""DI Resolver — resolves handler parameters at processing time.

See Contract 4 (Parameter Resolution Precedence) for the full rules.
"""

from __future__ import annotations

import inspect
import typing
from collections.abc import AsyncGenerator, Callable, Generator
from typing import Annotated, Any, get_args, get_origin

from rabbitkit.core.message import RabbitMessage
from rabbitkit.di.context import Context, ContextRepo, Header, Path
from rabbitkit.di.depends import Depends


class ConfigurationError(Exception):
    """Raised for invalid handler signatures."""


class DependencyScope:
    """Tracks generator dependencies for cleanup after handler completes."""

    def __init__(self) -> None:
        self._sync_generators: list[Generator[Any, None, None]] = []
        self._async_generators: list[AsyncGenerator[Any, None]] = []

    def add_sync_generator(self, gen: Generator[Any, None, None]) -> None:
        self._sync_generators.append(gen)

    def add_async_generator(self, gen: AsyncGenerator[Any, None]) -> None:
        self._async_generators.append(gen)

    def cleanup(self) -> None:
        """Close all sync generators (in reverse order)."""
        for gen in reversed(self._sync_generators):
            try:
                next(gen, None)
            except StopIteration:  # pragma: no cover
                pass  # pragma: no cover
            finally:
                gen.close()
        self._sync_generators.clear()

    async def cleanup_async(self) -> None:
        """Close all generators (async generators + sync generators, in reverse order)."""
        for gen in reversed(self._async_generators):
            try:
                await gen.__anext__()
            except StopAsyncIteration:
                pass
            finally:
                await gen.aclose()
        self._async_generators.clear()

        for sync_gen in reversed(self._sync_generators):
            try:
                next(sync_gen, None)
            except StopIteration:  # pragma: no cover
                pass  # pragma: no cover
            finally:
                sync_gen.close()
        self._sync_generators.clear()


class DIResolver:
    """Resolves handler parameters.

    Resolution rules (Contract 4):
    1. Annotated with DI marker (Depends/Header/Path/Context) → resolve via marker
    2. Type is RabbitMessage → inject the message object
    3. Remaining unannotated parameters → ONE body-bound parameter allowed
    4. Multiple body-like parameters → ConfigurationError at registration time
    5. Parameters with defaults → use default if no other resolution applies

    Constraint: At most one body-bound parameter per handler.
    """

    def _get_type_hints(self, handler: Callable[..., Any]) -> dict[str, Any]:
        """Get resolved type hints for handler, handling `from __future__ import annotations`.

        When annotations are strings (PEP 563), we need to evaluate them.
        For locally-defined handlers, typing.get_type_hints() may fail because
        local variables aren't in the module's global namespace.

        Strategy:
        1. Try typing.get_type_hints() with include_extras=True
        2. On failure, try with closure variables as localns
        3. Final fallback: use inspect.get_annotations with eval_str=True
        """
        try:
            return typing.get_type_hints(handler, include_extras=True)
        except Exception:
            pass

        # Attempt 2: try with closure variables as localns
        localns: dict[str, Any] = {}
        if hasattr(handler, "__code__") and hasattr(handler, "__closure__"):
            code = handler.__code__
            closure = handler.__closure__
            if closure is not None:
                for name, cell in zip(code.co_freevars, closure, strict=False):
                    try:
                        localns[name] = cell.cell_contents
                    except ValueError:  # pragma: no cover
                        pass  # pragma: no cover
        try:
            return typing.get_type_hints(handler, localns=localns, include_extras=True)
        except Exception:
            pass

        # Attempt 3: use inspect.get_annotations (Python 3.10+)
        try:
            return inspect.get_annotations(handler, eval_str=True)
        except Exception:
            pass

        # Final fallback: return raw annotations from signature
        sig = inspect.signature(handler)
        return {
            name: param.annotation
            for name, param in sig.parameters.items()
            if param.annotation is not inspect.Parameter.empty
        }

    def validate_handler(self, handler: Callable[..., Any]) -> None:
        """Validate handler signature at registration time.

        Raises ConfigurationError for:
        - *args or **kwargs
        - Multiple body-like parameters
        """
        sig = inspect.signature(handler)
        hints = self._get_type_hints(handler)
        body_params: list[str] = []

        for param_name, param in sig.parameters.items():
            # Reject *args and **kwargs
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                raise ConfigurationError(
                    f"Handler '{handler.__qualname__}': *args/**kwargs not supported. "
                    "Use explicit parameters with type annotations."
                )

            ann = hints.get(param_name, inspect.Parameter.empty)

            # Check if it's a DI-annotated parameter
            if self._has_di_marker(ann):
                continue

            # Check if it's RabbitMessage type
            if ann is RabbitMessage:
                continue

            # Check if it has a default (non-body)
            if param.default is not inspect.Parameter.empty:
                continue

            # This is a potential body parameter
            body_params.append(param_name)

        if len(body_params) > 1:
            raise ConfigurationError(
                f"Handler '{handler.__qualname__}': multiple body-like parameters "
                f"({', '.join(body_params)}). At most one body parameter allowed. "
                "Annotate extra parameters with Depends(), Header(), Path(), or Context()."
            )

    def resolve(
        self,
        handler: Callable[..., Any],
        message: RabbitMessage,
        context_repo: ContextRepo | None,
        body: Any,
        scope: DependencyScope | None = None,
    ) -> dict[str, Any]:
        """Resolve all handler parameters at message processing time."""
        sig = inspect.signature(handler)
        hints = self._get_type_hints(handler)
        kwargs: dict[str, Any] = {}
        depends_cache: dict[int, Any] = {}
        body_injected = False

        for param_name, param in sig.parameters.items():
            ann = hints.get(param_name, inspect.Parameter.empty)

            # Rule 1: DI-annotated parameters
            marker = self._extract_di_marker(ann)
            if marker is not None:
                if isinstance(marker, Depends):
                    kwargs[param_name] = self._resolve_depends(marker, depends_cache, scope)
                else:
                    kwargs[param_name] = self._resolve_marker(marker, message, context_repo, depends_cache)
                continue

            # Rule 2: RabbitMessage type
            if ann is RabbitMessage:
                kwargs[param_name] = message
                continue

            # Rule 3: Body-bound parameter (first one)
            if not body_injected and param.default is inspect.Parameter.empty:
                kwargs[param_name] = body
                body_injected = True
                continue

            # Rule 5: Parameters with defaults — omit (use default)

        return kwargs

    async def resolve_async(
        self,
        handler: Callable[..., Any],
        message: RabbitMessage,
        context_repo: ContextRepo | None,
        body: Any,
        scope: DependencyScope | None = None,
    ) -> dict[str, Any]:
        """Resolve all handler parameters, supporting async generator dependencies."""
        sig = inspect.signature(handler)
        hints = self._get_type_hints(handler)
        kwargs: dict[str, Any] = {}
        depends_cache: dict[int, Any] = {}
        body_injected = False

        for param_name, param in sig.parameters.items():
            ann = hints.get(param_name, inspect.Parameter.empty)

            # Rule 1: DI-annotated parameters
            marker = self._extract_di_marker(ann)
            if marker is not None:
                if isinstance(marker, Depends):
                    kwargs[param_name] = await self._resolve_depends_async(marker, depends_cache, scope)
                else:
                    kwargs[param_name] = self._resolve_marker(marker, message, context_repo, depends_cache)
                continue

            # Rule 2: RabbitMessage type
            if ann is RabbitMessage:
                kwargs[param_name] = message
                continue

            # Rule 3: Body-bound parameter (first one)
            if not body_injected and param.default is inspect.Parameter.empty:
                kwargs[param_name] = body
                body_injected = True
                continue

            # Rule 5: Parameters with defaults — omit (use default)

        return kwargs

    # ── Internal helpers ─────────────────────────────────────────────────

    def _has_di_marker(self, ann: Any) -> bool:
        """Check if annotation has a DI marker."""
        return self._extract_di_marker(ann) is not None

    def _extract_di_marker(self, ann: Any) -> Depends | Header | Path | Context | None:
        """Extract DI marker from annotation (supports Annotated)."""
        if ann is inspect.Parameter.empty:
            return None

        # Check Annotated[Type, Marker]
        if get_origin(ann) is Annotated:
            args = get_args(ann)
            for arg in args[1:]:
                if isinstance(arg, (Depends, Header, Path, Context)):
                    return arg
        return None

    def _resolve_marker(
        self,
        marker: Depends | Header | Path | Context,
        message: RabbitMessage,
        context_repo: ContextRepo | None,
        depends_cache: dict[int, Any],
    ) -> Any:
        """Resolve a single DI marker."""
        if isinstance(marker, Depends):
            return self._resolve_depends(marker, depends_cache)
        if isinstance(marker, Header):
            return self._resolve_header(marker, message)
        if isinstance(marker, Path):
            return self._resolve_path(marker, message)
        if isinstance(marker, Context):
            return self._resolve_context(marker, context_repo)
        raise ConfigurationError(f"Unknown DI marker: {marker!r}")

    def _resolve_depends(
        self, marker: Depends, cache: dict[int, Any], scope: DependencyScope | None = None
    ) -> Any:
        """Resolve a Depends() marker, with support for sync generator dependencies."""
        dep_id = id(marker.dependency)
        if marker.use_cache and dep_id in cache:
            return cache[dep_id]

        if inspect.isgeneratorfunction(marker.dependency):
            gen = marker.dependency()
            result = next(gen)
            if scope is not None:
                scope.add_sync_generator(gen)
        elif inspect.isasyncgenfunction(marker.dependency):
            raise ConfigurationError(
                f"Async generator dependency '{marker.dependency.__qualname__}' "
                "requires async pipeline. Use resolve_async() or ensure handler is async."
            )
        else:
            result = marker.dependency()

        if marker.use_cache:
            cache[dep_id] = result
        return result

    async def _resolve_depends_async(
        self, marker: Depends, cache: dict[int, Any], scope: DependencyScope | None = None
    ) -> Any:
        """Resolve a Depends() marker (async), with support for async/sync generator dependencies."""
        dep_id = id(marker.dependency)
        if marker.use_cache and dep_id in cache:
            return cache[dep_id]

        if inspect.isasyncgenfunction(marker.dependency):
            gen = marker.dependency()
            result = await gen.__anext__()
            if scope is not None:
                scope.add_async_generator(gen)
        elif inspect.isgeneratorfunction(marker.dependency):
            gen = marker.dependency()
            result = next(gen)
            if scope is not None:
                scope.add_sync_generator(gen)
        else:
            result = marker.dependency()
            if hasattr(result, "__await__"):
                result = await result

        if marker.use_cache:
            cache[dep_id] = result
        return result

    def _resolve_header(self, marker: Header, message: RabbitMessage) -> Any:
        """Resolve a Header() marker."""
        if marker.name not in message.headers:
            raise KeyError(f"Header '{marker.name}' not found in message headers")
        return message.headers[marker.name]

    def _resolve_path(self, marker: Path, message: RabbitMessage) -> Any:
        """Resolve a Path() marker."""
        if marker.segment not in message.path:
            raise KeyError(f"Path segment '{marker.segment}' not found in message path")
        return message.path[marker.segment]

    def _resolve_context(self, marker: Context, context_repo: ContextRepo | None) -> Any:
        """Resolve a Context() marker."""
        if context_repo is None:
            raise ConfigurationError("ContextRepo not available for Context() resolution")
        if not context_repo.has(marker.key):
            raise KeyError(f"Context key '{marker.key}' not found")
        return context_repo.get(marker.key)
