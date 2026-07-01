# Brokers

The broker is the main entry point. It wires the registry, pipeline, and transport.

## Import paths

```python
# Recommended — top-level re-export
from rabbitkit import AsyncBroker, SyncBroker

# rabbitkit.aio is a clean alias for rabbitkit.async_
from rabbitkit.aio import AsyncBroker

# Full path
from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.sync.broker import SyncBroker
```

## AsyncBroker

::: rabbitkit.async_.broker.AsyncBroker

## SyncBroker

::: rabbitkit.sync.broker.SyncBroker

## RabbitApp (lifecycle)

::: rabbitkit.core.app.RabbitApp
::: rabbitkit.core.app.AppState
