"""
kafka/consumer/event_consumer.py
==================================
Reads raw events from Kafka, validates them against our Pydantic schemas,
and hands them off to downstream processors (Spark, S3 writer, etc.).

CONSUMER CONCEPTS TO UNDERSTAND:
─────────────────────────────────
  Consumer Group:
    Multiple consumers can share the work of reading a topic.
    If a topic has 3 partitions and you have 3 consumers in the same group,
    each consumer reads exactly 1 partition → horizontal scaling!
    Group ID = "analytics-pipeline" means all instances share the load.

  Offset:
    Kafka assigns a sequential number (offset) to every message in a partition.
    The consumer tracks which offset it last processed. If the consumer crashes
    and restarts, it picks up from where it left off.
    Offset 0 → 1 → 2 → 3 ...  (never deleted by default)

  Commit:
    "I've finished processing up to offset N."
    We use enable.auto.commit=False → we commit ONLY after successfully
    writing to S3. This is the "at-least-once" delivery guarantee.

  Poll loop:
    Consumers don't get pushed messages. They poll() on a loop.
    poll(timeout=1.0) means: "give me any available messages, wait up to 1s"
"""

import json
import logging
import os
import signal
import sys
from typing import Any, Callable, Dict, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from data.schemas.events import (
    UserClickEvent, OrderEvent, InventoryCDCEvent,
    TOPIC_SCHEMA_MAP
)

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("kafka.consumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP_ID       = os.getenv("CONSUMER_GROUP_ID", "analytics-pipeline")


# =============================================================================
# DESERIALIZER — JSON bytes → validated Pydantic model
# =============================================================================

class EventDeserializer:
    """
    Converts raw Kafka message bytes into validated domain objects.

    Why validate here? Because Kafka is just a byte pipe — it has no idea
    if the data is valid. A misconfigured producer or a schema change could
    send garbage. We want to catch that at the ENTRY point of our pipeline,
    not deep inside Spark 30 minutes later.
    """

    def deserialize(self, topic: str, raw_bytes: bytes) -> Optional[Any]:
        """
        Returns a Pydantic model instance, or None if validation fails.
        Failed events are logged for dead-letter queue handling (future step).
        """
        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Cannot parse JSON from topic={topic}: {e}")
            return None

        schema_class = TOPIC_SCHEMA_MAP.get(topic)
        if schema_class is None:
            logger.warning(f"No schema registered for topic={topic}")
            return None

        try:
            return schema_class(**payload)
        except Exception as e:
            logger.error(f"Schema validation failed | topic={topic} | error={e}")
            logger.debug(f"Bad payload: {payload}")
            return None


# =============================================================================
# BASE CONSUMER — Wraps confluent-kafka with graceful shutdown + error handling
# =============================================================================

class EcommerceEventConsumer:
    """
    Generic Kafka consumer that:
    1. Subscribes to one or more topics
    2. Polls messages in a loop
    3. Deserializes + validates each message
    4. Calls a user-provided handler function
    5. Commits offsets only after handler succeeds
    6. Handles graceful shutdown on SIGTERM/SIGINT
    """

    def __init__(self, topics: list[str], handler: Callable[[Any, str], None]):
        """
        Args:
            topics:  List of Kafka topic names to subscribe to
            handler: Function called with (validated_event, topic_name)
        """
        self.topics       = topics
        self.handler      = handler
        self.deserializer = EventDeserializer()
        self._running     = True

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT,  self._handle_shutdown)

        self.consumer = Consumer({
            "bootstrap.servers":       KAFKA_BOOTSTRAP_SERVERS,
            "group.id":                CONSUMER_GROUP_ID,

            # auto.offset.reset: What to do when there's no committed offset?
            #   "earliest" → read from the very beginning of the topic
            #   "latest"   → only read NEW messages (skip history)
            "auto.offset.reset":       "earliest",

            # We manually commit after processing → exactly-once semantics
            # (well, "at-least-once" — we commit after writing to S3)
            "enable.auto.commit":      False,

            # How often to send heartbeats to Kafka (proves we're alive)
            "heartbeat.interval.ms":   3000,
            "session.timeout.ms":      30000,

            # Max time between poll() calls before Kafka considers us dead
            "max.poll.interval.ms":    300000,  # 5 minutes (for slow Spark jobs)
        })

        self.consumer.subscribe(topics)
        logger.info(f"Consumer subscribed to topics: {topics} | group={CONSUMER_GROUP_ID}")

    def _handle_shutdown(self, signum, frame):
        logger.info(f"Received signal {signum} — shutting down consumer...")
        self._running = False

    def run(self, batch_size: int = 100):
        """
        Main poll loop. Processes messages one by one.

        Args:
            batch_size: Commit offsets every N messages (reduces Kafka calls)
        """
        processed_count = 0
        error_count     = 0

        logger.info("🎧 Consumer started, listening for events...")

        try:
            while self._running:
                # Poll for a message (wait up to 1 second)
                msg: Optional[Message] = self.consumer.poll(timeout=1.0)

                if msg is None:
                    # No message available right now — normal, keep polling
                    continue

                if msg.error():
                    # Kafka system errors (partition EOF is normal, others aren't)
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        error_count += 1
                    continue

                # Deserialize and validate
                topic = msg.topic()
                event = self.deserializer.deserialize(topic, msg.value())

                if event is None:
                    # Invalid message → log and skip (dead-letter queue in future)
                    error_count += 1
                    self.consumer.commit(message=msg)  # Don't reprocess bad messages
                    continue

                # Hand off to handler (Spark writer, S3 writer, etc.)
                try:
                    self.handler(event, topic)
                    processed_count += 1

                    # Commit offset after successful processing
                    # This means: "I've processed up to this message"
                    if processed_count % batch_size == 0:
                        self.consumer.commit(asynchronous=True)
                        logger.info(
                            f"✅ Processed {processed_count} | "
                            f"Errors: {error_count} | "
                            f"Partition: {msg.partition()} | "
                            f"Offset: {msg.offset()}"
                        )

                except Exception as e:
                    logger.error(f"Handler error for event {getattr(event, 'event_id', '?')}: {e}")
                    error_count += 1
                    # Don't commit → message will be reprocessed on restart

        except KafkaException as e:
            logger.critical(f"Fatal Kafka error: {e}")
        finally:
            # Always commit final offsets and close cleanly
            self.consumer.commit()
            self.consumer.close()
            logger.info(f"Consumer closed. Total processed: {processed_count} | Errors: {error_count}")


# =============================================================================
# SIMPLE LOGGING HANDLER — for testing the consumer standalone
# =============================================================================

class LoggingHandler:
    """A simple handler that just logs events. Good for debugging."""

    def __call__(self, event: Any, topic: str):
        event_type = getattr(event, "event_type", "unknown")
        event_id   = getattr(event, "event_id", "?")
        timestamp  = getattr(event, "timestamp", "?")

        if topic == "ecommerce.user_clicks":
            logger.info(
                f"[CLICK ] {event_id[:8]} | "
                f"type={event.click_type.value:20} | "
                f"user={str(event.user_id)[:8] if event.user_id else 'anon   '} | "
                f"device={event.device_type.value}"
            )
        elif topic == "ecommerce.orders":
            logger.info(
                f"[ORDER ] {event.order_id} | "
                f"status={event.order_status.value:10} | "
                f"${event.total_amount:.2f} | "
                f"items={len(event.items)}"
            )
        elif topic == "ecommerce.inventory_cdc":
            product_id = event.after.product_id if event.after else event.before.product_id
            logger.info(
                f"[CDC   ] {event.operation.value:6} | "
                f"product={product_id} | "
                f"lsn={event.lsn}"
            )
        else:
            logger.info(f"[{event_type.upper():8}] {event_id[:8]} @ {timestamp}")


# =============================================================================
# ENTRY POINT — Run standalone to verify consumer works
# =============================================================================

if __name__ == "__main__":
    ALL_TOPICS = [
        "ecommerce.user_clicks",
        "ecommerce.orders",
        "ecommerce.inventory_cdc",
    ]

    handler  = LoggingHandler()
    consumer = EcommerceEventConsumer(topics=ALL_TOPICS, handler=handler)
    consumer.run()
