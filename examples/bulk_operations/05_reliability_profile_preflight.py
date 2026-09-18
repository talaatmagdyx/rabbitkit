"""Reliability profiles: build a CRITICAL config, run preflight, print policies.

``critical_config()`` fills in the profile's requirements (confirms,
mandatory, persistence, 256 KiB body cap, quorum retry chain) and refuses to
silently override a value you pinned to something the profile forbids.
``broker.preflight`` verifies what it can locally and, given a read-only
management client, verifies queue type / dead-letter-strategy / overflow /
delivery-limit on the broker. Anything it cannot verify is reported
UNVERIFIED — never green. ``policy_templates`` renders the reviewed policy
definitions your cluster owner applies (rabbitkit never mutates policies on
its own; ``RabbitManagementClient.put_policy`` is the deliberate step).

Run:
    python examples/bulk_operations/05_reliability_profile_preflight.py

Requirements:
    pip install "rabbitkit[sync]"
    RabbitMQ (management plugin) running on localhost:5672 / 15672
"""

from rabbitkit import (
    ConfigValidationError,
    ManagementConfig,
    PublisherConfig,
    QueueType,
    RabbitConfig,
    RabbitManagementClient,
    RabbitQueue,
    ReliabilityProfile,
    apply_profile,
    critical_config,
    policy_templates,
    validate_profile,
)
from rabbitkit.sync import SyncBroker

QUEUE = "bulk-demo-critical-orders"


def main() -> None:
    # 1. A contradiction is an error, not a silent override.
    try:
        apply_profile(RabbitConfig(publisher=PublisherConfig(confirm_delivery=False)), ReliabilityProfile.CRITICAL)
    except ConfigValidationError as exc:
        print(f"contradiction rejected: {exc}\n")

    # 2. Fill in the critical requirements on top of your own config.
    config = critical_config(RabbitConfig())
    print("critical publisher settings:", config.publisher)
    assert config.retry is not None
    print(f"retry chain: delay={config.retry.delay_queue_type} dlq={config.retry.dlq_queue_type}\n")

    broker = SyncBroker(config)

    @broker.subscriber(queue=RabbitQueue(name=QUEUE, queue_type=QueueType.QUORUM))
    def handle(body: bytes) -> None:
        pass

    # 3. Local validation lists every unmet requirement (empty = compliant).
    violations = validate_profile(config, ReliabilityProfile.CRITICAL, routes=broker.routes)
    print("local violations:", [str(v) for v in violations] or "none")

    # 4. Reviewed policy definitions for the cluster owner.
    print("\npolicies to apply (rabbitmqctl form):")
    for tpl in policy_templates([QUEUE], retry=config.retry, profile=ReliabilityProfile.CRITICAL):
        print("  " + tpl.as_rabbitmqctl())

    # 5. Preflight against the live broker via the management API.
    broker.start()
    try:
        mgmt = RabbitManagementClient(ManagementConfig(url="http://localhost:15672"))
        try:
            report = broker.preflight(ReliabilityProfile.CRITICAL, management_client=mgmt)
        except Exception as exc:  # management API unreachable → still a useful, honest report
            print(f"\nmanagement API unavailable ({type(exc).__name__}); local-only preflight:")
            report = broker.preflight(ReliabilityProfile.CRITICAL)
        print(f"\npreflight ok={report.ok} fully_verified={report.fully_verified}")
        for check in report.checks:
            print(f"  {check.status.value:<10} {check.name}  {check.detail}")
        if report.failed:
            print("\nFAILED checks above are usually the broker policies not being applied yet —")
            print("apply the templates printed earlier (or mgmt.put_policy(tpl.name, tpl)) and re-run.")
    finally:
        broker.stop()


if __name__ == "__main__":
    main()
