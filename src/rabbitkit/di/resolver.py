"""DI Resolver — resolves handler parameters at processing time.

See Contract 4 (Parameter Resolution Precedence) for the full rules.
"""

from __future__ import annotations

import inspect
import logging
import re
import typing
from collections.abc import AsyncGenerator, Callable, Generator
from typing import Annotated, Any, get_args, get_origin

from rabbitkit.core.errors import ConfigurationError, MissingDependencyError
from rabbitkit.core.message import RabbitMessage, is_rabbit_message_annotation
from rabbitkit.di.context import Context, ContextRepo, Header, Path
from rabbitkit.di.depends import Depends

_logger = logging.getLogger(__name__)

# L11: textual match on a DI marker CALL, used only once real hint resolution
# (get_type_hints_with_fallback's three attempts) has already failed and we
# are left with a raw, unresolved string annotation (PEP 563). See
# get_type_hints_with_fallback and DIResolver.validate_handler.
_DI_MARKER_CALL_RE = re.compile(r"\b(?:Depends|Header|Path|Context)\(")


def get_type_hints_with_fallback(handler: Callable[..., Any]) -> dict[str, Any]:
    """Resolve ``handler``'s type hints, tolerating ``from __future__ import
    annotations`` forward references that plain ``typing.get_type_hints()``
    can't reach on its own.

    When annotations are strings (PEP 563), they need evaluating. For
    locally-defined handlers (e.g. built by a factory function), the
    annotated type may live in the enclosing closure rather than the
    handler's module globals -- plain ``get_type_hints()`` can't see it.

    Strategy, each tried in order, first success wins:
    1. ``typing.get_type_hints()`` — the common case.
    2. ``typing.get_type_hints()`` with the handler's closure variables
       supplied as ``localns`` — resolves closure-scoped forward refs.
    3. ``inspect.get_annotations(eval_str=True)`` — a different evaluation
       path that occasionally succeeds where (1)/(2) don't.
    4. Final fallback: the raw ``inspect.signature()`` annotations, which
       are still PEP 563 strings if every real attempt above failed (e.g. a
       forward ref to a name that is genuinely unreachable, such as one only
       imported under ``if TYPE_CHECKING:``).

    This single implementation backs BOTH :class:`DIResolver` (the actual
    per-message resolver) and the pipeline's own DI-need detector
    (``HandlerPipeline._handler_needs_di``) — the two must use identical
    resolution strength. They used to diverge (the detector had a weaker,
    2-attempt version without the closure-``localns`` retry), which meant a
    closure-scoped ``Depends(...)`` annotation could resolve fine here but
    still be mis-detected as "no DI needed" by the weaker detector, silently
    binding the marked parameter to the message body instead (L11).
    """
    try:
        return typing.get_type_hints(handler, include_extras=True)
    except Exception:
        pass

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

    try:
        return inspect.get_annotations(handler, eval_str=True)
    except Exception:
        pass

    # Final fallback: raw (possibly still-a-string) annotations from signature.
    sig = inspect.signature(handler)
    return {
        name: param.annotation
        for name, param in sig.parameters.items()
        if param.annotation is not inspect.Parameter.empty
    }


def _looks_like_unresolved_di_marker(ann: Any) -> bool:
    """L11: best-effort detection of a DI marker in an annotation that
    :func:`get_type_hints_with_fallback` could not resolve to a real type
    (still a raw PEP 563 string after all three real resolution attempts).

    A plain string has no ``__metadata__``, so the structural
    ``Annotated[...]`` check used everywhere else can't see a marker here —
    this falls back to a textual match on the marker call syntax
    (``Depends(``/``Header(``/``Path(``/``Context(``). That's imperfect (an
    aliased import, e.g. ``from ... import Depends as D``, slips through),
    but a silent, wrong body-binding of a DI-marked parameter is worse than
    an occasional false positive turned into a clear registration-time error.
    """
    return isinstance(ann, str) and bool(_DI_MARKER_CALL_RE.search(ann))


def _is_rabbit_message(ann: Any) -> bool:
    """True if ``ann`` is/mentions :class:`RabbitMessage`.

    Handles both the resolved class and the string form (``"RabbitMessage"``)
    produced by ``from __future__ import annotations`` when the hint can't be
    resolved by ``typing.get_type_hints``. Recognizing the string form prevents
    valid ``(body: bytes, msg: RabbitMessage)`` handlers from being mis-classified
    as having two body-like parameters.
    """
    return is_rabbit_message_annotation(ann)


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
        """Close all sync generators (in reverse order).

        Each generator's teardown is isolated: a raising teardown is logged
        and skipped so one misbehaving generator does not leak the rest or
        prevent ``clear()`` from running.
        """
        for gen in reversed(self._sync_generators):
            try:
                next(gen, None)
            except StopIteration:  # pragma: no cover
                pass  # pragma: no cover
            except Exception:
                _logger.warning("sync generator teardown raised", exc_info=True)
            finally:
                try:
                    gen.close()
                except Exception:
                    _logger.warning("sync generator close() raised", exc_info=True)
        self._sync_generators.clear()

    async def cleanup_async(self) -> None:
        """Close all generators (async generators + sync generators, in reverse order).

        Each generator's teardown is isolated: a raising async teardown is
        logged and skipped, and the sync-generator pass always runs even if an
        async generator raised. ``clear()`` always runs.
        """
        for gen in reversed(self._async_generators):
            try:
                await gen.__anext__()
            except StopAsyncIteration:
                pass
            except Exception:
                _logger.warning("async generator teardown raised", exc_info=True)
            finally:
                try:
                    await gen.aclose()
                except Exception:
                    _logger.warning("async generator aclose() raised", exc_info=True)
        self._async_generators.clear()

        for sync_gen in reversed(self._sync_generators):
            try:
                next(sync_gen, None)
            except StopIteration:  # pragma: no cover
                pass  # pragma: no cover
            except Exception:
                _logger.warning("sync generator teardown raised", exc_info=True)
            finally:
                try:
                    sync_gen.close()
                except Exception:
                    _logger.warning("sync generator close() raised", exc_info=True)
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

    def __init__(self) -> None:
        # Per-handler cache of (signature, type-hints). Reflection — especially
        # typing.get_type_hints() — is expensive and STATIC per handler, so it is
        # computed once and reused on the per-message hot path.
        self._sig_hints_cache: dict[Any, tuple[inspect.Signature, dict[str, Any]]] = {}

    def _sig_and_hints(self, handler: Callable[..., Any]) -> tuple[inspect.Signature, dict[str, Any]]:
        cached = self._sig_hints_cache.get(handler)
        if cached is None:
            cached = (inspect.signature(handler), self._get_type_hints(handler))
            self._sig_hints_cache[handler] = cached
        return cached

    def _get_type_hints(self, handler: Callable[..., Any]) -> dict[str, Any]:
        """Get resolved type hints for handler. See :func:`get_type_hints_with_fallback`."""
        return get_type_hints_with_fallback(handler)

    def validate_handler(self, handler: Callable[..., Any]) -> None:
        """Validate handler signature at registration time.

        Raises ConfigurationError for:
        - *args or **kwargs
        - Multiple body-like parameters
        - An annotation that looks like an unresolved DI marker (L11) — see
          ``_looks_like_unresolved_di_marker``
        """
        sig, hints = self._sig_and_hints(handler)
        body_params: list[str] = []

        for param_name, param in sig.parameters.items():
            # Reject *args and **kwargs
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                raise ConfigurationError(
                    f"Handler '{handler.__qualname__}': *args/**kwargs not supported. "
                    "Use explicit parameters with type annotations."
                )

            ann = hints.get(param_name, inspect.Parameter.empty)

            # L11: get_type_hints_with_fallback() could not resolve this
            # annotation to a real type (it's still a raw PEP 563 string),
            # and it textually looks like a DI marker call. Silently
            # continuing would bind this parameter to the message body
            # instead of resolving it via the marker — fail fast instead.
            if _looks_like_unresolved_di_marker(ann):
                raise ConfigurationError(
                    f"Handler '{handler.__qualname__}': parameter '{param_name}' has an "
                    f"annotation ({ann!r}) that looks like it uses a DI marker "
                    "(Depends/Header/Path/Context) but rabbitkit could not resolve it to "
                    "a real type. This usually means the annotated type is not reachable "
                    "from the handler's module globals or enclosing closure -- e.g. it is "
                    "only imported under `if TYPE_CHECKING:`, or defined somewhere "
                    "typing.get_type_hints() can't see. Left unresolved, this parameter "
                    "would silently bind to the message body instead of the marker. Make "
                    "the annotated type resolvable (e.g. import it unconditionally) to fix."
                )

            # Check if it's a DI-annotated parameter
            if self._has_di_marker(ann):
                continue

            # Check if it's RabbitMessage type (class or string form)
            if _is_rabbit_message(ann):
                continue

            # Check if it has a default (non-body)
            if param.default is not inspect.Parameter.empty:
                continue

            # Unannotated parameters are NOT body-like candidates: the fallback
            # resolver binds the first unannotated param to the body and subsequent
            # unannotated params to the message (a documented pattern, e.g.
            # ``handle(body, msg)``). Only flag ANNOTATED params that look like body
            # types (a clear intent signal that multiple body bindings are expected,
            # which the resolver does not support) — e.g. ``handle(a: Order, b: Customer)``.
            if ann is inspect.Parameter.empty:
                continue

            # This is an annotated body-like parameter
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
        sig, hints = self._sig_and_hints(handler)
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
                    kwargs[param_name] = self._resolve_marker_with_fallback(
                        marker, param, param_name, message, context_repo
                    )
                continue

            # Rule 2: RabbitMessage type (class or string form)
            if _is_rabbit_message(ann):
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
        sig, hints = self._sig_and_hints(handler)
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
                    kwargs[param_name] = self._resolve_marker_with_fallback(
                        marker, param, param_name, message, context_repo
                    )
                continue

            # Rule 2: RabbitMessage type (class or string form)
            if _is_rabbit_message(ann):
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

    def _resolve_marker_with_fallback(
        self,
        marker: Header | Path | Context,
        param: inspect.Parameter,
        param_name: str,
        message: RabbitMessage,
        context_repo: ContextRepo | None,
    ) -> Any:
        """Resolve a Header()/Path()/Context() marker (H10).

        Fallback order when the value is absent from the message:
        1. The marker's own ``default=`` (checked first, inside
           ``_resolve_header``/``_resolve_path``/``_resolve_context``).
        2. The handler parameter's own Python default (checked here).
        3. Neither present → ``MissingDependencyError`` (PERMANENT).

        ``Depends()`` markers never reach here — both ``resolve()`` and
        ``resolve_async()`` special-case them before calling this.
        """
        try:
            if isinstance(marker, Header):
                return self._resolve_header(marker, message, param_name)
            if isinstance(marker, Path):
                return self._resolve_path(marker, message, param_name)
            if isinstance(marker, Context):
                return self._resolve_context(marker, context_repo, param_name)
            raise ConfigurationError(f"Unknown DI marker: {marker!r}")  # pragma: no cover - defensive
        except MissingDependencyError:
            if param.default is not inspect.Parameter.empty:
                return param.default
            raise

    def _resolve_depends(self, marker: Depends, cache: dict[int, Any], scope: DependencyScope | None = None) -> Any:
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

    def _resolve_header(self, marker: Header, message: RabbitMessage, param_name: str) -> Any:
        """Resolve a Header() marker (H10: marker default checked first)."""
        if marker.name in message.headers:
            return message.headers[marker.name]
        if marker.has_default:
            return marker.default
        raise MissingDependencyError(repr(marker), param_name)

    def _resolve_path(self, marker: Path, message: RabbitMessage, param_name: str) -> Any:
        """Resolve a Path() marker (H10: marker default checked first)."""
        if marker.segment in message.path:
            return message.path[marker.segment]
        if marker.has_default:
            return marker.default
        raise MissingDependencyError(repr(marker), param_name)

    def _resolve_context(self, marker: Context, context_repo: ContextRepo | None, param_name: str) -> Any:
        """Resolve a Context() marker (H10: marker default checked first)."""
        if context_repo is None:
            raise ConfigurationError("ContextRepo not available for Context() resolution")
        if context_repo.has(marker.key):
            return context_repo.get(marker.key)
        if marker.has_default:
            return marker.default
        raise MissingDependencyError(repr(marker), param_name)
