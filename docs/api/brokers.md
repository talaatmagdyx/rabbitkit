# Brokers

The broker is the main entry point. It wires the registry, pipeline, and transport.

## Import paths

```python
# Recommended — top-level re-export
from rabbitkit import AsyncBroker, SyncBroker

# Also fine — canonical submodule path
from rabbitkit.async_ import AsyncBroker
from rabbitkit.sync import SyncBroker

# Full path
from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.sync.broker import SyncBroker
```

`rabbitkit.aio` is a deprecated alias for `rabbitkit.async_` — importing it
emits a `DeprecationWarning`; use `rabbitkit.async_` instead.

## AsyncBroker

::: rabbitkit.async_.broker.AsyncBroker

## SyncBroker

::: rabbitkit.sync.broker.SyncBroker

## RabbitApp (lifecycle)

::: rabbitkit.core.app.RabbitApp
::: rabbitkit.core.app.AppState
