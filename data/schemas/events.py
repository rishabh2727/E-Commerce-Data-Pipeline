"""
data/schemas/events.py
======================
Pydantic data models for every event type flowing through the pipeline.

WHY PYDANTIC?
  - Validates incoming JSON at the boundary (Kafka consumer)
  - Auto-generates JSON Schema docs
  - Catches bad data BEFORE it pollutes the Data Lake

EVENT TYPES WE TRACK:
  1. UserClickEvent   — page views, button clicks, search queries
  2. OrderEvent       — cart additions, checkouts, payments
  3. InventoryEvent   — CDC (Change Data Capture) from the warehouse DB
  4. ProcessedEvent   — enriched event after Spark processing
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator


# =============================================================================
# ENUMS — Controlled vocabularies so we never get surprise strings
# =============================================================================

class ClickEventType(str, Enum):
    PAGE_VIEW      = "page_view"
    PRODUCT_VIEW   = "product_view"
    SEARCH         = "search"
    ADD_TO_CART    = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    WISHLIST_ADD   = "wishlist_add"
    CHECKOUT_START = "checkout_start"


class OrderStatus(str, Enum):
    PENDING    = "pending"
    CONFIRMED  = "confirmed"
    SHIPPED    = "shipped"
    DELIVERED  = "delivered"
    CANCELLED  = "cancelled"
    REFUNDED   = "refunded"


class CDCOperation(str, Enum):
    """
    CDC = Change Data Capture.
    When the inventory DB changes a row, we capture:
      INSERT → new product added
      UPDATE → stock level changed
      DELETE → product removed
    """
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class DeviceType(str, Enum):
    MOBILE  = "mobile"
    DESKTOP = "desktop"
    TABLET  = "tablet"


# =============================================================================
# BASE MODEL — All events share these fields
# =============================================================================

class BaseEvent(BaseModel):
    """
    Every single event in the system carries these fields.
    This is the "envelope" around the actual payload.
    """
    event_id:   str      = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    timestamp:  datetime = Field(default_factory=datetime.utcnow)
    session_id: str
    user_id:    Optional[str] = None   # None for anonymous users

    class Config:
        # Allow datetime objects to be serialized to ISO strings
        json_encoders = {datetime: lambda v: v.isoformat()}


# =============================================================================
# 1. USER CLICK EVENTS
#    Source: website JavaScript tracker → Kafka topic: ecommerce.user_clicks
# =============================================================================

class UserClickEvent(BaseEvent):
    """
    Fired every time a user interacts with the website.
    Volume: Very high (~thousands/second on a real site)
    """
    event_type: str = "user_click"

    # What did the user click?
    click_type:   ClickEventType
    page_url:     str
    referrer_url: Optional[str] = None
    product_id:   Optional[str] = None   # Set when click_type is PRODUCT_VIEW
    category:     Optional[str] = None
    search_query: Optional[str] = None   # Set when click_type is SEARCH

    # User context
    device_type:  DeviceType
    browser:      Optional[str] = None
    country_code: Optional[str] = None   # "US", "IN", "GB" etc.
    ip_address:   Optional[str] = None

    # Marketing attribution
    utm_source:   Optional[str] = None   # "google", "facebook"
    utm_medium:   Optional[str] = None   # "cpc", "email"
    utm_campaign: Optional[str] = None

    @validator("page_url")
    def url_must_not_be_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("page_url cannot be empty")
        return v


# =============================================================================
# 2. ORDER EVENTS
#    Source: checkout service → Kafka topic: ecommerce.orders
# =============================================================================

class OrderItem(BaseModel):
    """A single line item within an order."""
    product_id:   str
    product_name: str
    category:     str
    quantity:     int   = Field(gt=0)       # Must be positive
    unit_price:   float = Field(gt=0.0)     # Must be positive
    discount_pct: float = Field(ge=0.0, le=100.0)  # 0–100%

    @property
    def line_total(self) -> float:
        return self.quantity * self.unit_price * (1 - self.discount_pct / 100)


class OrderEvent(BaseEvent):
    """
    Fired at key order lifecycle moments (status changes).
    Volume: Medium (~hundreds/second)
    """
    event_type: str = "order"

    order_id:      str
    order_status:  OrderStatus
    items:         List[OrderItem]
    total_amount:  float = Field(ge=0.0)
    currency:      str   = "USD"

    # Shipping
    shipping_country: Optional[str] = None
    shipping_city:    Optional[str] = None

    # Payment
    payment_method: Optional[str] = None  # "credit_card", "paypal", "crypto"

    @validator("total_amount", always=True)
    def validate_total(cls, v, values):
        """Cross-check: total_amount should match sum of line items."""
        if "items" in values and values["items"]:
            calculated = sum(item.line_total for item in values["items"])
            # Allow 1 cent rounding difference
            if abs(v - calculated) > 0.01:
                raise ValueError(
                    f"total_amount {v} doesn't match calculated {calculated:.2f}"
                )
        return v


# =============================================================================
# 3. INVENTORY CDC EVENTS
#    Source: Debezium connector watching PostgreSQL → Kafka topic: ecommerce.inventory_cdc
#
#    CDC = Change Data Capture
#    Instead of polling the DB every minute ("what changed?"),
#    CDC watches the database's binary log and streams every change instantly.
#    This means near-zero latency between DB update and our pipeline knowing about it.
# =============================================================================

class InventoryBeforeAfter(BaseModel):
    """Snapshot of a row BEFORE and AFTER a change."""
    product_id:     str
    product_name:   str
    sku:            str
    stock_quantity: int
    reorder_level:  int    # Alert when stock drops below this
    warehouse_id:   str
    last_updated:   datetime


class InventoryCDCEvent(BaseEvent):
    """
    Represents a single change to the inventory database.
    The 'before' field is None for INSERT operations.
    The 'after' field is None for DELETE operations.
    """
    event_type: str = "inventory_cdc"

    operation:  CDCOperation
    table_name: str = "inventory"
    before:     Optional[InventoryBeforeAfter] = None  # State BEFORE change
    after:      Optional[InventoryBeforeAfter] = None  # State AFTER change
    lsn:        Optional[str] = None  # Log Sequence Number from Postgres WAL

    @validator("after", always=True)
    def check_operation_fields(cls, v, values):
        op = values.get("operation")
        if op == CDCOperation.INSERT and v is None:
            raise ValueError("INSERT operation must have 'after' data")
        if op == CDCOperation.DELETE and values.get("before") is None:
            raise ValueError("DELETE operation must have 'before' data")
        return v


# =============================================================================
# 4. PROCESSED EVENT — Output from Spark
#    After Spark enriches raw events, it writes this to the processed layer
# =============================================================================

class ProcessedEvent(BaseModel):
    """
    The enriched, clean version of any event after Spark processing.
    This is what gets loaded into Redshift for analytics queries.
    """
    event_id:       str
    event_type:     str
    timestamp:      datetime
    user_id:        Optional[str]
    session_id:     str

    # Enrichment fields added by Spark
    processing_timestamp: datetime = Field(default_factory=datetime.utcnow)
    is_bot:              bool = False          # ML model prediction
    user_segment:        Optional[str] = None  # "high_value", "at_risk", etc.
    geo_region:          Optional[str] = None  # Derived from IP → region
    raw_payload:         Dict[str, Any] = {}   # Original event data

    # Data quality flags
    has_missing_fields:  bool = False
    quality_score:       float = Field(default=1.0, ge=0.0, le=1.0)

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# =============================================================================
# KAFKA TOPIC MAPPING — Which model lives on which topic
# =============================================================================

TOPIC_SCHEMA_MAP = {
    "ecommerce.user_clicks":    UserClickEvent,
    "ecommerce.orders":         OrderEvent,
    "ecommerce.inventory_cdc":  InventoryCDCEvent,
    "ecommerce.processed_events": ProcessedEvent,
}
