"""two_stage_chain — receive -> operate -> publish on; next queue: operate -> ACK.

The flow (one self-contained script: seeds, runs both stages, verifies, exits):

    seed producer ──> readings.raw ──> convert() ──> readings.celsius ──> store() ──> ack
                                       [stage 1]                          [stage 2]

Stage 1 (``readings.raw``) — receive raw sensor data, do the operation
(Fahrenheit -> Celsius + anomaly flag), and publish the result to the next
queue via ``@publisher``. The source message is acked ONLY after the
forwarded publish is broker-confirmed — a lost forward nack-requeues the
source instead of vanishing.

Stage 2 (``readings.celsius``) — ``AckPolicy.MANUAL``: the message is acked
by YOUR code, only after the operation (here: persisting to a store)
actually succeeds. If the store is briefly down, ``msg.nack(requeue=True)``
puts the reading back for redelivery — nothing is lost, nothing is acked
"on faith". This is the policy to reach for when the ack must be tied to a
side effect completing, not to the handler merely returning.

The script proves it end to end: one reading's first store attempt fails on
purpose -> it is nacked -> redelivered -> stored -> acked; the final store
holds every reading exactly once.

Run:
    docker run -d --rm -p 5672:5672 rabbitmq:3.13-management-alpine
    python examples/two_stage_chain/chain.py

Requirements:
    pip install "rabbitkit[sync]"
"""

import json
import os
import threading
import time

from rabbitkit import ConnectionConfig, ConsumerConfig, MessageEnvelope, RabbitConfig, WorkerConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import AckPolicy
from rabbitkit.sync import SyncBroker

READINGS = int(os.environ.get("READINGS", "10"))

broker = SyncBroker(
    RabbitConfig(
        connection=ConnectionConfig(
            host=os.environ.get("RABBITMQ_HOST", "localhost"),
            port=int(os.environ.get("RABBITMQ_PORT", "5672")),
        ),
        consumer=ConsumerConfig(prefetch_count=16),
    )
)


# ── Stage 1: receive raw -> operate -> publish to the next queue ────────────
# @publisher (inner) + @subscriber (outer): the handler's return value is
# published to readings.celsius with confirms, and the raw message is acked
# only after that publish is broker-confirmed.

@broker.subscriber(queue="readings.raw")
@broker.publisher(routing_key="readings.celsius")
def convert(body: bytes) -> bytes:
    reading = json.loads(body)
    celsius = round((reading["temp_f"] - 32) * 5 / 9, 2)
    result = {
        "sensor_id": reading["sensor_id"],
        "temp_c": celsius,
        "anomaly": celsius > 45.0,  # the "operation": convert + flag
        "taken_at": reading["taken_at"],
    }
    return json.dumps(result).encode()


# ── Stage 2: operate -> ACK (manual) ────────────────────────────────────────
# The ack is tied to the SIDE EFFECT succeeding, not to the handler returning.

store: dict[str, dict] = {}
store_lock = threading.Lock()
flaky_first_attempt_done = threading.Event()  # simulate one transient store failure
all_stored = threading.Event()
deliveries: dict[str, int] = {}


def save_to_store(event: dict) -> None:
    """The stage-2 operation. Fails ONCE for sensor-7 to prove the
    nack -> redeliver -> ack path. A real version writes to a database —
    keyed on sensor_id+taken_at so a redelivered duplicate overwrites
    instead of double-inserting (idempotent)."""
    if event["sensor_id"] == "sensor-7" and not flaky_first_attempt_done.is_set():
        flaky_first_attempt_done.set()
        raise ConnectionError("store briefly unavailable")
    with store_lock:
        store[event["sensor_id"]] = event
        if len(store) >= READINGS:
            all_stored.set()


@broker.subscriber(queue="readings.celsius", ack_policy=AckPolicy.MANUAL)
def persist(body: bytes, msg: RabbitMessage) -> None:
    event = json.loads(body)
    with store_lock:
        deliveries[event["sensor_id"]] = deliveries.get(event["sensor_id"], 0) + 1
    try:
        save_to_store(event)
    except ConnectionError:
        # Operation failed transiently: DO NOT ack — put it back for redelivery.
        msg.nack(requeue=True)
        return
    except (KeyError, ValueError):
        # Malformed payload: rerunning can never succeed — reject (no requeue,
        # so it dead-letters if the queue has a DLX, or is dropped otherwise).
        msg.reject(requeue=False)
        return
    # Only now — the side effect is durable — acknowledge.
    msg.ack()


# ── Seed + run + verify ─────────────────────────────────────────────────────

def main() -> None:
    # worker_count > 1: stage 1 publishes with confirms from its handler,
    # which on a single sync worker cannot be time-bounded (rabbitkit warns).
    broker.start(worker_config=WorkerConfig(worker_count=4))

    print(f"[seed] publishing {READINGS} raw readings ...")
    for i in range(READINGS):
        outcome = broker.publish(MessageEnvelope(
            routing_key="readings.raw",
            body=json.dumps({
                "sensor_id": f"sensor-{i}",
                "temp_f": 70 + i * 6,  # sensor-8/9 exceed 45C -> anomaly=True
                "taken_at": f"2026-07-13T10:00:{i:02d}Z",
            }).encode(),
            message_id=f"reading-{i}",
        ))
        assert outcome.ok, f"seed publish {i} failed: {outcome.status}"

    # Drive the consume loop; wait until stage 2 has stored everything.
    consume_thread = threading.Thread(target=broker._transport.start_consuming, daemon=True)
    consume_thread.start()
    t0 = time.monotonic()

    finished = all_stored.wait(timeout=60)

    broker._transport._connection.add_callback_threadsafe(broker._transport.stop_consuming)
    consume_thread.join(timeout=10)
    broker.stop()

    assert finished, f"only {len(store)}/{READINGS} readings stored"
    # The flaky reading was nacked once, redelivered, then stored + acked:
    assert deliveries["sensor-7"] >= 2, "expected sensor-7 to be redelivered after the nack"
    assert store["sensor-7"]["temp_c"] == round((70 + 7 * 6 - 32) * 5 / 9, 2)
    assert store["sensor-9"]["anomaly"] is True and store["sensor-0"]["anomaly"] is False

    print(
        f"[verified] {READINGS} readings: raw -> convert -> celsius -> store -> ack "
        f"in {time.monotonic() - t0:.1f}s; sensor-7 was nacked once, redelivered "
        f"({deliveries['sensor-7']} deliveries), stored exactly once."
    )


if __name__ == "__main__":
    main()
