# High-Load Infrastructure

## FlowController (backpressure)

::: rabbitkit.highload.backpressure.FlowController

## BatchPublisher

::: rabbitkit.highload.batch.BatchPublisher

## BatchAcker

Default mode is `individual` (one `multiple=False` frame per tag). The legacy
cumulative `ack(max_tag, multiple=True)` is opt-in via
`BatchAckConfig(mode="cumulative", ordered_exclusive_owner=True)`. For safe
coalescing under arbitrary completion order see
[`CoalescingAcker`](bulk.md#safe-coalescing).

::: rabbitkit.highload.batch.BatchAcker

## Flush accounting

::: rabbitkit.highload.batch.FlushReport
::: rabbitkit.highload.batch.FlushItem
::: rabbitkit.highload.batch.BatchFlushError
::: rabbitkit.highload.batch.BatchClosedError

## Worker Pools

::: rabbitkit.concurrency.SyncWorkerPool
::: rabbitkit.concurrency.AsyncWorkerPool
::: rabbitkit.core.config.WorkerConfig

## SyncBatchPublisher

Pipelined publisher confirms for sync code on a dedicated
`SelectConnection` I/O thread — raises the ~0.9k msg/s blocking-confirm
ceiling for callers who adopt it. Standalone by design (not wired into
`SyncBroker.publish`).

::: rabbitkit.sync.batch.SyncBatchPublisher
