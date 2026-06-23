"""order-service — the reference application from docs/rabbitmq-retry-architecture.md.

Real, typed, test-backed modules demonstrating the production retry patterns:
safe error classification, correctly-wired RetryMiddleware, idempotency, DLQ
tooling. The pure-logic and pipeline behaviour is covered by tests under
``tests/examples/order_service/``; infra wiring (real broker, FastAPI, mgmt API)
is constructed lazily so importing this package never requires a live broker.
"""
