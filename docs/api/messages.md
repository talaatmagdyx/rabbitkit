# Messages & Settlement

## RabbitMessage

::: rabbitkit.core.message.RabbitMessage

## MessageEnvelope

::: rabbitkit.core.types.MessageEnvelope

## Event metadata (optional)

See [Event Metadata](../event-metadata.md).

::: rabbitkit.core.event_metadata.EventMetadata
::: rabbitkit.core.event_metadata.check_supported_version
::: rabbitkit.core.event_metadata.format_occurred_at
::: rabbitkit.core.errors.EventMetadataError
::: rabbitkit.core.errors.InboundMessageTooLargeError

## Reserved headers

::: rabbitkit.core.reserved_headers.strip_reserved_headers

## AckPolicy

::: rabbitkit.core.types.AckPolicy

## AckStrategy

::: rabbitkit.core.types.AckStrategy

## PublishOutcome / PublishStatus

::: rabbitkit.core.types.PublishOutcome
::: rabbitkit.core.types.PublishStatus
::: rabbitkit.core.types.PublishReason

## Settlement exceptions

::: rabbitkit.core.message.AckMessage
::: rabbitkit.core.message.NackMessage
::: rabbitkit.core.message.RejectMessage
