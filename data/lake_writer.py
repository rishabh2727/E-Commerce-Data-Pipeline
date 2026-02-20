"""
data/lake_writer.py
====================
Writes validated events to MinIO (local S3) in the Raw Layer of the Data Lake.

DATA LAKE LAYERS (Medallion Architecture):
──────────────────────────────────────────
  🥉 Bronze (Raw)    → Exact copy of source data, nothing changed
                       s3://ecommerce-raw-events/
                       Format: JSON (preserves original exactly)
                       Retention: 90 days
                       
  🥈 Silver (Clean)  → Validated, deduplicated, type-cast
                       s3://ecommerce-processed/
                       Format: Parquet (columnar, compressed)
                       Retention: 1 year
                       
  🥇 Gold (Curated)  → Business-ready aggregations for dashboards
                       s3://ecommerce-curated/
                       Format: Parquet (partitioned by date+category)
                       Retention: Forever

WHY KEEP RAW DATA?
  If a bug in your Spark job corrupts the Silver layer,
  you can reprocess from Bronze. Raw data is your source of truth.

THIS FILE:
  Handles the Bronze layer write — the Kafka consumer calls this
  before handing events to Spark for Silver layer processing.
"""

import gzip
import io
import json
import logging
import os
from datetime import datetime
from typing import Any, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger("data.lake_writer")

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

RAW_BUCKET       = os.getenv("RAW_BUCKET",       "ecommerce-raw-events")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "ecommerce-processed")
CURATED_BUCKET   = os.getenv("CURATED_BUCKET",   "ecommerce-curated")


# =============================================================================
# S3 CLIENT FACTORY
# =============================================================================

def create_s3_client():
    """
    Creates a boto3 S3 client pointed at MinIO.
    In production: remove endpoint_url and it connects to real AWS S3.
    
    boto3 is the official AWS Python SDK.
    The same code works for both MinIO (local) and real S3 (cloud).
    That's the beauty of the S3-compatible API.
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(
            retries={"max_attempts": 3, "mode": "adaptive"},
            connect_timeout=5,
            read_timeout=30,
        ),
    )


# =============================================================================
# RAW EVENT WRITER (Bronze Layer)
# =============================================================================

class RawEventWriter:
    """
    Writes raw JSON events to the Bronze layer of the Data Lake.
    
    FILE NAMING STRATEGY:
      s3://ecommerce-raw-events/
        user_clicks/
          year=2024/month=01/day=15/hour=14/
            clicks_20240115_140523_abc123.json.gz
        orders/
          year=2024/month=01/day=15/
            orders_20240115_142301_def456.json.gz
        inventory_cdc/
          year=2024/month=01/day=15/
            cdc_20240115_143000_ghi789.json.gz
    
    BATCHING:
      Writing one file per event = millions of tiny files → S3 throttling nightmare.
      We buffer events in memory and flush to S3 every N events OR every T seconds.
      This is called "micro-batching" — same concept as Spark's trigger interval.
    """

    def __init__(self, batch_size: int = 100, flush_interval_sec: int = 30):
        self.s3          = create_s3_client()
        self.batch_size  = batch_size
        self.flush_interval = flush_interval_sec
        self._buffers: dict[str, list] = {}   # topic → list of event dicts
        self._last_flush: dict[str, datetime] = {}

    def _get_s3_key(self, topic: str, event_type: str) -> str:
        """
        Generate the S3 object key (path) for a given event.
        Uses Hive-style partitioning for efficient querying.
        
        Example: user_clicks/year=2024/month=01/day=15/hour=14/clicks_20240115_140523.json.gz
        """
        now = datetime.utcnow()
        folder_map = {
            "ecommerce.user_clicks":    "user_clicks",
            "ecommerce.orders":         "orders",
            "ecommerce.inventory_cdc":  "inventory_cdc",
        }
        folder = folder_map.get(topic, "unknown")
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")

        return (
            f"{folder}/"
            f"year={now.year}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"hour={now.hour:02d}/"
            f"{folder}_{timestamp_str}.json.gz"
        )

    def _flush_buffer(self, topic: str):
        """
        Serialize the buffer to gzip-compressed JSON Lines and upload to S3.
        
        JSON Lines format: one JSON object per line
          {"event_id": "abc", "click_type": "page_view", ...}
          {"event_id": "def", "click_type": "product_view", ...}
          
        Why JSON Lines? Each line is independently parseable.
        Spark, Athena, and Glue all support it natively.
        Gzip reduces file size by ~70%.
        """
        buffer = self._buffers.get(topic, [])
        if not buffer:
            return

        # Serialize to JSON Lines
        lines = "\n".join(json.dumps(event, default=str) for event in buffer)

        # Gzip compress in memory (no temp files!)
        compressed = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed, mode="wb") as gz:
            gz.write(lines.encode("utf-8"))
        compressed.seek(0)

        # Upload to S3/MinIO
        key = self._get_s3_key(topic, buffer[0].get("event_type", "unknown"))
        try:
            self.s3.put_object(
                Bucket=RAW_BUCKET,
                Key=key,
                Body=compressed.getvalue(),
                ContentType="application/gzip",
                Metadata={
                    "event_count": str(len(buffer)),
                    "topic":       topic,
                    "written_at":  datetime.utcnow().isoformat(),
                }
            )
            logger.info(
                f"📤 Flushed {len(buffer):4d} events → s3://{RAW_BUCKET}/{key}"
            )
        except ClientError as e:
            logger.error(f"S3 upload failed for {key}: {e}")
            raise
        finally:
            # Always clear buffer after attempting flush
            self._buffers[topic] = []
            self._last_flush[topic] = datetime.utcnow()

    def write(self, event: Any, topic: str):
        """
        Buffer an event. Flushes if batch_size reached or flush_interval elapsed.
        Called by the Kafka consumer's handler.
        """
        if topic not in self._buffers:
            self._buffers[topic] = []
            self._last_flush[topic] = datetime.utcnow()

        # Convert Pydantic model to dict
        if hasattr(event, "dict"):
            event_dict = json.loads(event.json())  # Handles datetime serialization
        else:
            event_dict = event

        self._buffers[topic].append(event_dict)

        # Check flush conditions
        buffer_full = len(self._buffers[topic]) >= self.batch_size
        time_elapsed = (
            (datetime.utcnow() - self._last_flush[topic]).seconds >= self.flush_interval
        )

        if buffer_full or time_elapsed:
            reason = "batch_full" if buffer_full else "time_elapsed"
            logger.debug(f"Flushing {topic} buffer ({reason})")
            self._flush_buffer(topic)

    def flush_all(self):
        """Force-flush all buffers. Call on shutdown."""
        for topic in list(self._buffers.keys()):
            if self._buffers[topic]:
                self._flush_buffer(topic)
        logger.info("All buffers flushed.")


# =============================================================================
# DATA LAKE CATALOG — Metadata about what's in our lake
# =============================================================================

class DataLakeCatalog:
    """
    Provides a simple API to inspect the Data Lake contents.
    In production this would be AWS Glue Data Catalog or Apache Iceberg.
    
    For now: just S3 prefix listing with statistics.
    """

    def __init__(self):
        self.s3 = create_s3_client()

    def list_partitions(self, bucket: str, prefix: str = "") -> List[dict]:
        """
        List all Hive-style partitions under a prefix.
        Returns metadata about each partition (size, file count, date range).
        """
        paginator = self.s3.get_paginator("list_objects_v2")
        partitions = []

        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
                for common_prefix in page.get("CommonPrefixes", []):
                    prefix_path = common_prefix["Prefix"]
                    # Count files and total size in this partition
                    sub_response = self.s3.list_objects_v2(
                        Bucket=bucket, Prefix=prefix_path
                    )
                    files      = sub_response.get("Contents", [])
                    total_size = sum(f["Size"] for f in files)
                    partitions.append({
                        "partition": prefix_path,
                        "file_count": len(files),
                        "total_size_mb": round(total_size / 1_000_000, 2),
                    })
        except ClientError as e:
            logger.error(f"Cannot list partitions: {e}")

        return partitions

    def get_lake_summary(self) -> dict:
        """Print a summary of the entire Data Lake."""
        summary = {}
        for bucket in [RAW_BUCKET, PROCESSED_BUCKET, CURATED_BUCKET]:
            try:
                response = self.s3.list_objects_v2(Bucket=bucket)
                objects = response.get("Contents", [])
                total_size = sum(o["Size"] for o in objects)
                summary[bucket] = {
                    "object_count": len(objects),
                    "total_size_mb": round(total_size / 1_000_000, 2),
                }
            except ClientError:
                summary[bucket] = {"error": "bucket not accessible"}
        return summary


# =============================================================================
# STANDALONE TEST — Run this to test the writer against local MinIO
# =============================================================================

if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

    from kafka.producer.event_producer import (
        generate_click_event, generate_order_event, generate_inventory_cdc_event
    )

    logging.basicConfig(level=logging.INFO)
    writer = RawEventWriter(batch_size=10)  # Small batch for testing

    print("Writing 25 test events to MinIO...")
    for i in range(20):
        writer.write(generate_click_event(), "ecommerce.user_clicks")
    for i in range(3):
        event = generate_order_event()
        if event:
            writer.write(event, "ecommerce.orders")
    for i in range(5):
        writer.write(generate_inventory_cdc_event(), "ecommerce.inventory_cdc")

    writer.flush_all()

    catalog = DataLakeCatalog()
    print("\n📊 Data Lake Summary:")
    for bucket, stats in catalog.get_lake_summary().items():
        print(f"  {bucket}: {stats}")
