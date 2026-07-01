# Stability Policy

## Pre-1.0 Status

RabbitKit is currently pre-1.0. The public API is stabilizing but breaking changes may occur between minor versions (0.x → 0.y). Each breaking change is documented in `CHANGELOG.md` with a migration note.

Version 1.0 will mark the point at which the stable API is frozen and semver guarantees apply: breaking changes only in major versions, new features in minor versions, bug fixes in patch versions.

---

## Stable APIs

The following symbols are considered stable. They will not be removed or changed in a backward-incompatible way within the 0.x series without a deprecation cycle (minimum one minor version of warning).

| Symbol | Module |
|---|---|
| `RabbitConfig` | `rabbitkit` |
| `AsyncBroker` | `rabbitkit` |
| `SyncBroker` | `rabbitkit` |
| `subscriber` | `rabbitkit` |
| `publisher` | `rabbitkit` |
| `RabbitMessage` | `rabbitkit` |
| `MessageEnvelope` | `rabbitkit` |
| `AckPolicy` | `rabbitkit` |
| `RetryConfig` | `rabbitkit` |
| `RabbitQueue` | `rabbitkit` |
| `RabbitExchange` | `rabbitkit` |
| `TestBroker` | `rabbitkit.testing` |
| `broker_health_check` | `rabbitkit` |
| `broker_health_check_async` | `rabbitkit` |

Stable means:

- The symbol exists at the listed import path.
- The constructor/call signature does not gain required parameters.
- Existing keyword arguments are not removed or renamed.
- Return types do not change in a breaking way.

---

## Experimental APIs

The following features are available under `rabbitkit.experimental` or via opt-in imports. They may change without notice in any 0.x release. Do not depend on them in production code unless you are prepared to track changes closely.

| Feature | Import path | Status |
|---|---|---|
| RPC (request/reply) | `rabbitkit.rpc` | Experimental |
| Dashboard (web UI) | `rabbitkit.dashboard` | Experimental |
| Stream queues | `rabbitkit.streams` | Experimental |
| Distributed locking | `rabbitkit.locking` | Experimental |
| Message signing | `rabbitkit.middleware.signing` | Experimental |
| Result backends | `rabbitkit.results` | Experimental |

Experimental APIs:

- May change signatures between patch releases.
- May be removed if the design proves unworkable.
- Are not covered by the deprecation policy below.

Feedback on experimental APIs is welcome and actively used to determine whether they graduate to stable.

---

## Deprecation Policy

For stable APIs:

1. A deprecation warning (`DeprecationWarning`) is emitted for at least one minor release before removal.
2. The `CHANGELOG.md` entry for the deprecating release documents the replacement.
3. The symbol is removed in the following minor release at the earliest.

Example timeline: deprecated in 0.6.0, removed no earlier than 0.7.0.

For experimental APIs, no deprecation cycle is guaranteed. Changes are noted in `CHANGELOG.md` but may take effect in the same release that introduces them.

---

## How to Track Changes

- `CHANGELOG.md` in the repository root is the authoritative record of changes per release.
- The `[Unreleased]` section shows what is planned or in progress for the next release.
- Breaking changes are marked with `**Breaking**` in the changelog entry.
