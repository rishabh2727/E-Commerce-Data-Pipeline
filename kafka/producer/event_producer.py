"""
kafka/producer/event_producer.py
=================================
Simulates a real e-commerce website by generating and publishing events to Kafka.

HOW A REAL PRODUCER WORKS:
  Real site: User clicks button → JS sends event to your tracking API →
             API validates & publishes to Kafka → pipeline consumes it.

  Here:      Python script generates realistic fake events →
             publishes to Kafka → same pipeline consumes it.

KAFKA PRODUCER KEY CONCEPTS:
  - bootstrap_servers: Where to find Kafka (like a phone book for brokers)
  - key:               Partition routing key (we use user_id so same user's
                       events always go to the same partition = ordered)
  - value:             The actual event payload (JSON bytes)
  - acks='all':        Wait for ALL replicas to confirm before considering
                       the message "sent" (strongest durability guarantee)
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import Optional

from confluent_kafka import Producer, KafkaException
from faker import Faker

# We import our Pydantic models to generate valid events
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from data.schemas.events import (
    UserClickEvent, OrderEvent, OrderItem, InventoryCDCEvent,
    InventoryBeforeAfter, ClickEventType, OrderStatus,
    CDCOperation, DeviceType
)

# ---------------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("kafka.producer")

# ---------------------------------------------------------------------------
# CONFIG — reads from environment or falls back to local defaults
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENTS_PER_SECOND       = float(os.getenv("EVENTS_PER_SECOND", "5"))

# ---------------------------------------------------------------------------
# FAKE DATA GENERATOR
# ---------------------------------------------------------------------------
fake = Faker()

# Realistic product catalog
PRODUCTS = [
    {"id": "P001", "name": "Wireless Headphones", "category": "Electronics", "price": 79.99},
    {"id": "P002", "name": "Running Shoes",        "category": "Sports",      "price": 129.99},
    {"id": "P003", "name": "Coffee Maker",         "category": "Kitchen",     "price": 49.99},
    {"id": "P004", "name": "Python Programming Book", "category": "Books",    "price": 34.99},
    {"id": "P005", "name": "Yoga Mat",             "category": "Sports",      "price": 25.99},
    {"id": "P006", "name": "Smart Watch",          "category": "Electronics", "price": 249.99},
    {"id": "P007", "name": "Desk Lamp",            "category": "Home",        "price": 19.99},
    {"id": "P008", "name": "Water Bottle",         "category": "Sports",      "price": 14.99},
]

WAREHOUSES = ["WH-US-EAST", "WH-US-WEST", "WH-EU-CENTRAL", "WH-APAC"]

# Active "sessions" to simulate realistic user journeys
# (same user browses → adds to cart → checks out)
_active_sessions: dict = {}


# =============================================================================
# EVENT GENERATORS
# =============================================================================

def _get_or_create_session() -> dict:
    """
    Maintain a pool of ~20 concurrent sessions.
    Each session = one user browsing the site right now.
    """
    # Expire old sessions (older than 10 minutes)
    now = datetime.utcnow()
    expired = [sid for sid, s in _active_sessions.items()
               if (now - s["last_active"]).seconds > 600]
    for sid in expired:
        del _active_sessions[sid]

    # Add new session if pool is small
    while len(_active_sessions) < 20:
        sid = fake.uuid4()
        _active_sessions[sid] = {
            "session_id":  sid,
            "user_id":     fake.uuid4() if random.random() > 0.3 else None,
            "device":      random.choice(list(DeviceType)),
            "country":     random.choice(["US", "IN", "GB", "DE", "CA", "AU"]),
            "last_active": now,
            "cart":        [],
        }

    session = random.choice(list(_active_sessions.values()))
    session["last_active"] = now
    return session


def generate_click_event() -> UserClickEvent:
    """Generate a realistic user click event."""
    session = _get_or_create_session()
    product = random.choice(PRODUCTS)

    # Weight the click types to reflect real usage patterns
    click_weights = {
        ClickEventType.PAGE_VIEW:     30,
        ClickEventType.PRODUCT_VIEW:  25,
        ClickEventType.SEARCH:        15,
        ClickEventType.ADD_TO_CART:   10,
        ClickEventType.REMOVE_FROM_CART: 3,
        ClickEventType.WISHLIST_ADD:   7,
        ClickEventType.CHECKOUT_START: 10,
    }
    click_type = random.choices(
        list(click_weights.keys()),
        weights=list(click_weights.values())
    )[0]

    # Track cart additions for later order generation
    if click_type == ClickEventType.ADD_TO_CART:
        session["cart"].append(product)

    return UserClickEvent(
        session_id=session["session_id"],
        user_id=session["user_id"],
        click_type=click_type,
        page_url=f"https://shop.example.com/products/{product['id']}",
        referrer_url=random.choice([
            "https://google.com", "https://facebook.com", None, None, None
        ]),
        product_id=product["id"] if click_type in [
            ClickEventType.PRODUCT_VIEW,
            ClickEventType.ADD_TO_CART,
            ClickEventType.WISHLIST_ADD,
        ] else None,
        category=product["category"],
        search_query=fake.word() if click_type == ClickEventType.SEARCH else None,
        device_type=session["device"],
        browser=random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
        country_code=session["country"],
        ip_address=fake.ipv4(),
        utm_source=random.choice(["google", "facebook", "email", None, None]),
        utm_medium=random.choice(["cpc", "organic", "email", None]),
        utm_campaign=random.choice(["summer_sale", "retargeting", None, None]),
    )


def generate_order_event() -> Optional[OrderEvent]:
    """
    Generate an order event. Only fires if a session has items in cart.
    This creates a realistic purchase funnel: browse → cart → order.
    """
    sessions_with_cart = [
        s for s in _active_sessions.values()
        if len(s["cart"]) > 0 and s["user_id"] is not None
    ]
    if not sessions_with_cart:
        return None

    session = random.choice(sessions_with_cart)
    cart_items = session["cart"][:3]  # Max 3 items per order for simulation

    items = [
        OrderItem(
            product_id=p["id"],
            product_name=p["name"],
            category=p["category"],
            quantity=random.randint(1, 3),
            unit_price=p["price"],
            discount_pct=random.choice([0, 0, 0, 10, 15, 20]),
        )
        for p in cart_items
    ]
    total = sum(i.line_total for i in items)

    # Clear the cart after order
    session["cart"] = []

    return OrderEvent(
        session_id=session["session_id"],
        user_id=session["user_id"],
        order_id=f"ORD-{fake.uuid4()[:8].upper()}",
        order_status=random.choices(
            list(OrderStatus),
            weights=[40, 30, 15, 10, 4, 1]
        )[0],
        items=items,
        total_amount=round(total, 2),
        shipping_country=session["country"],
        shipping_city=fake.city(),
        payment_method=random.choice(["credit_card", "paypal", "debit_card"]),
    )


def generate_inventory_cdc_event() -> InventoryCDCEvent:
    """
    Simulates a Change Data Capture event from the inventory database.

    In production this would come from Debezium watching PostgreSQL's
    Write-Ahead Log (WAL) — every INSERT/UPDATE/DELETE gets streamed here.
    """
    product = random.choice(PRODUCTS)
    operation = random.choices(
        [CDCOperation.UPDATE, CDCOperation.INSERT, CDCOperation.DELETE],
        weights=[80, 15, 5]
    )[0]

    def make_inventory(stock: int) -> InventoryBeforeAfter:
        return InventoryBeforeAfter(
            product_id=product["id"],
            product_name=product["name"],
            sku=f"SKU-{product['id']}-{fake.uuid4()[:4].upper()}",
            stock_quantity=stock,
            reorder_level=10,
            warehouse_id=random.choice(WAREHOUSES),
            last_updated=datetime.utcnow(),
        )

    before_stock = random.randint(5, 100)
    after_stock  = random.randint(0, 100)

    return InventoryCDCEvent(
        session_id=fake.uuid4(),  # CDC events don't have sessions, using random
        operation=operation,
        before=make_inventory(before_stock) if operation != CDCOperation.INSERT else None,
        after=make_inventory(after_stock)  if operation != CDCOperation.DELETE else None,
        lsn=f"0/1{fake.random_int(1000000, 9999999)}",
    )


# =============================================================================
# KAFKA PRODUCER
# =============================================================================

class EcommerceEventProducer:
    """
    Wraps confluent-kafka's Producer with our domain logic.

    KEY DECISIONS:
      - key = user_id (or session_id for anon users)
        → Kafka uses this to assign events to partitions
        → Same user's events always land on same partition = guaranteed order
      - value = JSON bytes
        → Simple, human-readable, no schema registry needed for now
      - acks='all'
        → Strongest guarantee: message is written to all replicas
    """

    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers":       KAFKA_BOOTSTRAP_SERVERS,
            "acks":                    "all",
            "retries":                 3,
            "retry.backoff.ms":        500,
            "linger.ms":               10,     # Batch messages for 10ms before sending
            "batch.size":              16384,  # 16KB batch size
            "compression.type":        "snappy",  # Compress to reduce network overhead
            "enable.idempotence":      True,   # Prevent duplicate messages on retry
        })
        logger.info(f"Producer connected to {KAFKA_BOOTSTRAP_SERVERS}")

    def _delivery_callback(self, err, msg):
        """
        Called by Kafka after each message is acknowledged (or fails).
        This is asynchronous — it fires after produce() returns.
        """
        if err:
            logger.error(f"❌ Delivery failed: {err} | topic={msg.topic()}")
        else:
            logger.debug(
                f"✅ Delivered | topic={msg.topic()} "
                f"partition={msg.partition()} offset={msg.offset()}"
            )

    def publish(self, topic: str, event, partition_key: Optional[str] = None):
        """
        Serialize the Pydantic model to JSON and publish to Kafka.
        """
        key   = (partition_key or event.session_id).encode("utf-8")
        value = event.json().encode("utf-8")

        try:
            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )
            # poll() drives the delivery callbacks; call often to avoid buffer overflow
            self.producer.poll(0)
        except KafkaException as e:
            logger.error(f"Produce error: {e}")
            raise

    def flush(self):
        """Wait until all buffered messages are delivered."""
        self.producer.flush()


# =============================================================================
# MAIN LOOP — Run this to start pumping events into Kafka
# =============================================================================

def run_producer():
    """
    Infinite loop that generates a realistic mix of events at a controlled rate.
    Mix ratio: 70% clicks, 20% orders, 10% inventory CDC
    """
    producer = EcommerceEventProducer()
    event_counts = {"clicks": 0, "orders": 0, "inventory": 0}
    interval = 1.0 / EVENTS_PER_SECOND

    logger.info(f"🚀 Starting producer at {EVENTS_PER_SECOND} events/second")
    logger.info("Topics: ecommerce.user_clicks | ecommerce.orders | ecommerce.inventory_cdc")

    try:
        while True:
            roll = random.random()

            if roll < 0.70:
                # 70% — User click events (highest volume traffic)
                event = generate_click_event()
                producer.publish("ecommerce.user_clicks", event)
                event_counts["clicks"] += 1
                logger.info(f"📱 CLICK  | {event.click_type.value:20} | user={event.user_id or 'anon'}")

            elif roll < 0.90:
                # 20% — Order events
                event = generate_order_event()
                if event:
                    producer.publish("ecommerce.orders", event)
                    event_counts["orders"] += 1
                    logger.info(f"🛒 ORDER  | {event.order_id} | status={event.order_status.value} | ${event.total_amount:.2f}")

            else:
                # 10% — Inventory CDC events
                event = generate_inventory_cdc_event()
                producer.publish("ecommerce.inventory_cdc", event)
                event_counts["inventory"] += 1
                logger.info(f"📦 CDC    | {event.operation.value:6} | product={event.after.product_id if event.after else event.before.product_id}")

            # Print summary every 50 events
            total = sum(event_counts.values())
            if total % 50 == 0:
                logger.info(
                    f"📊 TOTALS → clicks={event_counts['clicks']} | "
                    f"orders={event_counts['orders']} | "
                    f"inventory={event_counts['inventory']}"
                )

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
        producer.flush()
        logger.info(f"Final counts: {event_counts}")


if __name__ == "__main__":
    run_producer()
