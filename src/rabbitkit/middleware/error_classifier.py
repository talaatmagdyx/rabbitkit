"""ErrorClassifierMiddleware — pluggable error classification.

NOT a standalone middleware in the chain. Used internally by RetryMiddleware.
Wraps core/errors.py classify_error() with configurable per-route overrides.
"""

from __future__ import annotations

from collections.abc import Sequence

from rabbitkit.core.errors import ErrorPredicate, classify_error
from rabbitkit.core.types import ClassifiedError, ErrorSeverity


class ErrorClassifierMiddleware:
    """Error classification component used by RetryMiddleware.

    Pluggable via predicates: Callable[[BaseException], bool | None]
    True=transient, False=permanent, None=no opinion (fall through).

    unknown_policy configurable (default=PERMANENT). See Contract 7.
    """

    def __init__(
        self,
        *,
        predicates: Sequence[ErrorPredicate] = (),
        unknown_policy: ErrorSeverity = ErrorSeverity.PERMANENT,
    ) -> None:
        self._predicates = list(predicates)
        self._unknown_policy = unknown_policy

    def classify(self, exc: BaseException) -> ClassifiedError:
        """Classify an exception's severity."""
        return classify_error(
            exc,
            predicates=self._predicates,
            unknown_policy=self._unknown_policy,
        )

    @property
    def unknown_policy(self) -> ErrorSeverity:
        return self._unknown_policy
