# Core Internals

## HandlerPipeline

::: rabbitkit.core.pipeline.HandlerPipeline

## SubscriberRegistry

::: rabbitkit.core.registry.SubscriberRegistry
::: rabbitkit.core.registry.DuplicateRouteError

## RouteDefinition

::: rabbitkit.core.route.RouteDefinition
::: rabbitkit.core.route.ResultPublisher
::: rabbitkit.core.route.ConfigurationError

## Error Classification

::: rabbitkit.core.errors.classify_error
::: rabbitkit.core.errors.ErrorSeverity
::: rabbitkit.core.errors.ClassifiedError

## Exceptions

All raised for caller-side mistakes; the validation errors dual-inherit
`ValueError` and the runtime-misuse errors dual-inherit `RuntimeError`, so
pre-existing builtin catches keep working. See the
[exception taxonomy](../guide/full-guide.md#exception-taxonomy) for the
full table and catch patterns.

::: rabbitkit.core.errors.ConfigurationError
::: rabbitkit.core.errors.ConfigValidationError
::: rabbitkit.core.errors.TopologyValidationError
::: rabbitkit.core.errors.UnsafeTopologyError
::: rabbitkit.core.errors.MessageTooLargeError
::: rabbitkit.core.errors.BrokerNotStartedError
::: rabbitkit.core.message.SettlementError
::: rabbitkit.core.errors.MissingDependencyError
::: rabbitkit.core.errors.BackpressureError
::: rabbitkit.core.errors.PublishError

## Protocols

::: rabbitkit.core.protocols.Transport
::: rabbitkit.core.protocols.AsyncTransport
::: rabbitkit.core.protocols.SupportsBackpressure

## Types

::: rabbitkit.core.types.ExchangeType
::: rabbitkit.core.types.QueueType
::: rabbitkit.core.types.AppState

## Logging

::: rabbitkit.core.logging.LoggingConfig
::: rabbitkit.core.logging.configure_structlog

## Path extraction

::: rabbitkit.core.path.extract_path
::: rabbitkit.core.path.to_binding_key
