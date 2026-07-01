# Security Policy

## Supported Versions

rabbitkit follows [SemVer](https://semver.org/). Security fixes are backported
to the latest minor release on the current major version; older majors are
not supported.

| Version | Supported          |
| ------- | ------------------ |
| 1.x     | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Report suspected vulnerabilities privately by emailing
**talaatmagdy75@gmail.com** with:

- A description of the vulnerability and its potential impact.
- Steps to reproduce (a minimal repro is ideal — a script or failing test
  against `TestBroker` or a local RabbitMQ instance).
- The affected version(s) and, if known, the affected file/function.

If the repository has GitHub's [private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing/privately-reporting-a-security-vulnerability)
enabled, you may instead open a report via the **Security** tab → **Report a
vulnerability**.

### What to expect

- **Acknowledgement** within 5 business days.
- An initial assessment (severity, affected versions) within 10 business
  days of acknowledgement.
- A fix or mitigation plan communicated before any public disclosure.
  Coordinated disclosure is expected — please give us a reasonable window
  (typically 90 days, sooner for actively-exploited issues) to ship a fix
  before disclosing publicly.

## Scope

In scope: the `rabbitkit` package itself (`src/rabbitkit/`), including its
sync (pika) and async (aio-pika) transports, serialization, middleware
(signing, deduplication, rate limiting, compression), and the CLI.

Out of scope: vulnerabilities in RabbitMQ itself, third-party dependencies
(report those upstream), or issues that require an already-compromised
broker/network to exploit.

## Known Security-Relevant Design Notes

These are documented behaviors, not vulnerabilities, but are worth reading
before relying on them for security-sensitive traffic:

- `SigningMiddleware`'s default in-memory `TTLSetNonceCache` is per-process —
  in a multi-process/multi-pod deployment (or after a restart) a replayed
  message routed to a different worker will not be detected. Pass a shared
  `nonce_cache=RedisNonceCache(...)` for replay protection across processes.
- `WorkerConfig.stop_timeout`: a handler still running past this deadline is
  *abandoned*, not killed (Python cannot forcibly stop an arbitrary thread).
  Handlers must be idempotent under at-least-once delivery.
