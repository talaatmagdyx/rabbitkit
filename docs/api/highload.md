# High-Load Infrastructure

## FlowController (backpressure)

::: rabbitkit.highload.backpressure.FlowController

## BatchPublisher

::: rabbitkit.highload.batch.BatchPublisher

## BatchAcker

::: rabbitkit.highload.batch.BatchAcker

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
