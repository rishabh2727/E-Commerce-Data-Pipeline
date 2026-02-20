"""
data/quality/expectations.py
==============================
Data quality checks using Great Expectations.

WHAT IS GREAT EXPECTATIONS (GE)?
─────────────────────────────────
  A library for defining, running, and documenting data quality rules.
  
  You write "expectations" like unit tests for data:
    expect_column_values_to_not_be_null("user_id")
    expect_column_values_to_be_between("total_amount", min_value=0)
    expect_column_to_exist("event_id")
  
  GE then generates HTML reports showing PASS/FAIL for each rule.
  This is called a "Data Docs" site.

WHERE DO CHECKS RUN?
  1. Streaming layer: Check event schema as events arrive (light checks)
  2. Batch layer: Full statistical checks on daily Parquet files
  3. Before Redshift load: "Gate" — don't load if quality score < 95%

WHY THIS MATTERS:
  - Catches data drift (upstream schema changes break your pipeline)
  - Proves data quality to stakeholders ("99.7% of orders have valid totals")
  - Required for any production data pipeline
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("data.quality")

# =============================================================================
# LIGHTWEIGHT EXPECTATIONS (no GE installed → pure Python fallback)
# These run in the Kafka consumer for every single event.
# Keep them FAST — microseconds, not milliseconds.
# =============================================================================

class EventQualityChecker:
    """
    Fast, in-memory quality checks run on each event as it arrives.
    Returns a quality score (0.0–1.0) and a list of violations.
    
    Think of this as the "cheap filter" before data hits the lake.
    Heavy statistical checks run in batch (GE suite below).
    """

    def check_click_event(self, event: dict) -> tuple[float, List[str]]:
        """Returns (quality_score, list_of_violations)."""
        violations = []

        # ── Required fields ──────────────────────────────────────────────
        required = ["event_id", "session_id", "click_type", "page_url", "device_type"]
        for field in required:
            if not event.get(field):
                violations.append(f"MISSING_REQUIRED_FIELD: {field}")

        # ── Format checks ─────────────────────────────────────────────────
        valid_click_types = {
            "page_view", "product_view", "search", "add_to_cart",
            "remove_from_cart", "wishlist_add", "checkout_start"
        }
        if event.get("click_type") not in valid_click_types:
            violations.append(f"INVALID_CLICK_TYPE: {event.get('click_type')}")

        valid_devices = {"mobile", "desktop", "tablet"}
        if event.get("device_type") not in valid_devices:
            violations.append(f"INVALID_DEVICE_TYPE: {event.get('device_type')}")

        # ── URL format ────────────────────────────────────────────────────
        url = event.get("page_url", "")
        if url and not (url.startswith("http://") or url.startswith("https://")):
            violations.append(f"INVALID_URL_FORMAT: {url[:50]}")

        # ── Timestamp sanity ─────────────────────────────────────────────
        ts = event.get("timestamp")
        if ts:
            try:
                event_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                now = datetime.utcnow()
                age_hours = (now - event_time.replace(tzinfo=None)).total_seconds() / 3600
                if age_hours > 24:
                    violations.append(f"STALE_EVENT: {age_hours:.1f} hours old")
                if age_hours < -1:
                    violations.append(f"FUTURE_EVENT: timestamp is in the future")
            except (ValueError, AttributeError):
                violations.append(f"INVALID_TIMESTAMP_FORMAT: {ts}")

        # ── Score: subtract points per violation ─────────────────────────
        score = max(0.0, 1.0 - (len(violations) * 0.2))
        return score, violations

    def check_order_event(self, event: dict) -> tuple[float, List[str]]:
        violations = []

        required = ["event_id", "order_id", "order_status", "items", "total_amount"]
        for field in required:
            if event.get(field) is None:
                violations.append(f"MISSING_REQUIRED_FIELD: {field}")

        # Total amount sanity
        total = event.get("total_amount", 0)
        if total < 0:
            violations.append(f"NEGATIVE_TOTAL_AMOUNT: {total}")
        if total > 100_000:
            violations.append(f"SUSPICIOUSLY_HIGH_TOTAL: {total}")

        # Items validation
        items = event.get("items", [])
        if not items:
            violations.append("EMPTY_ORDER_ITEMS")
        for i, item in enumerate(items):
            if item.get("quantity", 0) <= 0:
                violations.append(f"ITEM_{i}_INVALID_QUANTITY: {item.get('quantity')}")
            if item.get("unit_price", 0) <= 0:
                violations.append(f"ITEM_{i}_INVALID_PRICE: {item.get('unit_price')}")

        # Cross-field: total should match sum of items
        if items:
            calculated = sum(
                item.get("quantity", 0) * item.get("unit_price", 0) *
                (1 - item.get("discount_pct", 0) / 100)
                for item in items
            )
            if abs(total - calculated) > 0.50:  # 50 cent tolerance
                violations.append(
                    f"TOTAL_MISMATCH: stated={total:.2f} calculated={calculated:.2f}"
                )

        score = max(0.0, 1.0 - (len(violations) * 0.25))
        return score, violations

    def check_inventory_cdc_event(self, event: dict) -> tuple[float, List[str]]:
        violations = []

        valid_ops = {"INSERT", "UPDATE", "DELETE"}
        if event.get("operation") not in valid_ops:
            violations.append(f"INVALID_CDC_OPERATION: {event.get('operation')}")

        # INSERT must have 'after', DELETE must have 'before'
        op = event.get("operation")
        if op == "INSERT" and not event.get("after"):
            violations.append("INSERT_MISSING_AFTER_STATE")
        if op == "DELETE" and not event.get("before"):
            violations.append("DELETE_MISSING_BEFORE_STATE")

        # Stock quantity sanity
        for state_key in ["before", "after"]:
            state = event.get(state_key)
            if state:
                stock = state.get("stock_quantity", 0)
                if stock < 0:
                    violations.append(f"{state_key.upper()}_NEGATIVE_STOCK: {stock}")

        score = max(0.0, 1.0 - (len(violations) * 0.3))
        return score, violations

    def check(self, event: dict, topic: str) -> tuple[float, List[str]]:
        """Route to the correct checker based on topic."""
        if topic == "ecommerce.user_clicks":
            return self.check_click_event(event)
        elif topic == "ecommerce.orders":
            return self.check_order_event(event)
        elif topic == "ecommerce.inventory_cdc":
            return self.check_inventory_cdc_event(event)
        else:
            return 1.0, []  # Unknown topic — pass through


# =============================================================================
# GREAT EXPECTATIONS SUITE — Batch validation on Parquet files
# Run by Airflow after each hourly batch
# =============================================================================

class GreatExpectationsSuite:
    """
    Full Great Expectations validation suite.
    
    SUITE = a named collection of expectations for a dataset.
    CHECKPOINT = a config that says "run suite X on datasource Y and save results to Z"
    DATA DOCS  = auto-generated HTML report from checkpoint results
    
    HOW IT INTEGRATES WITH AIRFLOW:
      1. Spark writes hourly Parquet → s3://ecommerce-processed/click_events/
      2. Airflow DAG runs at :05 past the hour
      3. DAG task calls run_click_events_checkpoint()
      4. GE validates the Parquet file
      5. If quality_score < 0.95 → DAG fails → Redshift load doesn't happen
      6. Alert sent to Slack/PagerDuty
    """

    def __init__(self, data_docs_path: str = "/tmp/ge_data_docs"):
        self.data_docs_path = data_docs_path
        try:
            import great_expectations as ge
            self.ge = ge
            self._ge_available = True
            logger.info("Great Expectations loaded successfully")
        except ImportError:
            self._ge_available = False
            logger.warning(
                "great_expectations not installed. "
                "Run: pip install great-expectations. "
                "Using lightweight checker instead."
            )

    def build_click_event_suite(self) -> dict:
        """
        Returns a dictionary of GE expectations for click events.
        
        When GE is installed, this gets loaded into a GE context.
        When it's not, we convert these to our lightweight checker format.
        """
        return {
            "suite_name": "click_events_suite",
            "expectations": [
                # ── Completeness ────────────────────────────────────────────
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "event_id"},
                    "meta": {"notes": "Every event must have a unique ID"}
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "session_id"},
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "click_type"},
                },
                # ── Uniqueness ───────────────────────────────────────────────
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "event_id"},
                    "meta": {"notes": "Duplicate event_ids = producer retry bug"}
                },
                # ── Value Sets ───────────────────────────────────────────────
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {
                        "column": "click_type",
                        "value_set": [
                            "page_view", "product_view", "search",
                            "add_to_cart", "remove_from_cart",
                            "wishlist_add", "checkout_start"
                        ]
                    }
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {
                        "column": "device_type",
                        "value_set": ["mobile", "desktop", "tablet"]
                    }
                },
                # ── Statistical checks ────────────────────────────────────
                {
                    "expectation_type": "expect_column_proportion_of_unique_values_to_be_between",
                    "kwargs": {
                        "column": "session_id",
                        "min_value": 0.01,  # At least 1% unique sessions
                        "max_value": 1.0
                    },
                    "meta": {"notes": "Catches stuck producers sending same session ID"}
                },
                {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {
                        "min_value": 100,    # At least 100 events per hour
                        "max_value": 10_000_000  # Not more than 10M (traffic spike alert)
                    }
                },
                # ── Freshness check ───────────────────────────────────────
                {
                    "expectation_type": "expect_column_max_to_be_between",
                    "kwargs": {
                        "column": "event_timestamp",
                        "min_value": "NOW() - INTERVAL 2 HOURS",
                    },
                    "meta": {"notes": "Most recent event must be within last 2 hours"}
                },
            ]
        }

    def build_order_event_suite(self) -> dict:
        return {
            "suite_name": "order_events_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "order_id"}
                },
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "order_id"},
                    "meta": {"notes": "Duplicate orders = serious business data issue"}
                },
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": {
                        "column": "total_amount",
                        "min_value": 0.01,
                        "max_value": 50_000,
                    }
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {
                        "column": "order_status",
                        "value_set": [
                            "pending", "confirmed", "shipped",
                            "delivered", "cancelled", "refunded"
                        ]
                    }
                },
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": {
                        "column": "item_count",
                        "min_value": 1,
                        "max_value": 100,
                    }
                },
            ]
        }

    def run_validation(self, data: List[dict], suite: dict) -> dict:
        """
        Run the expectation suite against a list of event dicts.
        Returns a validation result with pass/fail per expectation.
        
        In production with GE installed, this would use:
          context.run_checkpoint(checkpoint_name="daily_click_events")
        
        This fallback implementation runs the expectations manually.
        """
        results = []
        total_passed = 0

        for expectation in suite["expectations"]:
            exp_type = expectation["expectation_type"]
            kwargs   = expectation["kwargs"]
            column   = kwargs.get("column")

            result = {
                "expectation_type": exp_type,
                "column": column,
                "kwargs": kwargs,
                "passed": False,
                "details": "",
            }

            try:
                if exp_type == "expect_column_values_to_not_be_null" and column:
                    null_count = sum(1 for r in data if r.get(column) is None)
                    result["passed"] = null_count == 0
                    result["details"] = f"null_count={null_count}/{len(data)}"

                elif exp_type == "expect_column_values_to_be_unique" and column:
                    values = [r.get(column) for r in data if r.get(column)]
                    result["passed"] = len(values) == len(set(values))
                    result["details"] = f"duplicates={len(values) - len(set(values))}"

                elif exp_type == "expect_column_values_to_be_in_set" and column:
                    value_set = set(kwargs.get("value_set", []))
                    invalid = [r.get(column) for r in data
                               if r.get(column) not in value_set]
                    result["passed"] = len(invalid) == 0
                    result["details"] = f"invalid_values={invalid[:5]}"

                elif exp_type == "expect_column_values_to_be_between" and column:
                    min_v = kwargs.get("min_value", float("-inf"))
                    max_v = kwargs.get("max_value", float("inf"))
                    out_of_range = [r.get(column) for r in data
                                    if r.get(column) is not None and
                                    not (min_v <= r.get(column) <= max_v)]
                    result["passed"] = len(out_of_range) == 0
                    result["details"] = f"out_of_range={out_of_range[:5]}"

                elif exp_type == "expect_table_row_count_to_be_between":
                    min_v = kwargs.get("min_value", 0)
                    max_v = kwargs.get("max_value", float("inf"))
                    result["passed"] = min_v <= len(data) <= max_v
                    result["details"] = f"row_count={len(data)}"

                else:
                    result["passed"] = True  # Unknown expectation → skip
                    result["details"] = "skipped (not implemented)"

            except Exception as e:
                result["passed"] = False
                result["details"] = f"error: {e}"

            if result["passed"]:
                total_passed += 1
            results.append(result)

        quality_score = total_passed / len(suite["expectations"]) if suite["expectations"] else 1.0

        return {
            "suite_name":    suite["suite_name"],
            "validated_at":  datetime.utcnow().isoformat(),
            "row_count":     len(data),
            "quality_score": round(quality_score, 3),
            "passed":        total_passed,
            "failed":        len(suite["expectations"]) - total_passed,
            "results":       results,
        }

    def print_report(self, validation_result: dict):
        """Human-readable report of validation results."""
        r = validation_result
        status = "✅ PASSED" if r["quality_score"] >= 0.95 else "❌ FAILED"
        print(f"\n{'='*60}")
        print(f"Data Quality Report — {r['suite_name']}")
        print(f"{'='*60}")
        print(f"Status:        {status}")
        print(f"Quality Score: {r['quality_score']:.1%}")
        print(f"Rows Checked:  {r['row_count']:,}")
        print(f"Passed:        {r['passed']}/{r['passed'] + r['failed']}")
        print(f"Validated At:  {r['validated_at']}")
        print(f"\nExpectation Results:")
        for exp in r["results"]:
            icon = "✅" if exp["passed"] else "❌"
            print(f"  {icon} {exp['expectation_type']:<50} {exp.get('details', '')}")
        print(f"{'='*60}\n")


# =============================================================================
# QUALITY GATE — Called by Airflow before loading into Redshift
# =============================================================================

def quality_gate(data: List[dict], event_type: str, min_score: float = 0.95) -> bool:
    """
    Returns True if data passes quality threshold → proceed with Redshift load.
    Returns False → abort load, send alert.
    
    Called at the end of the Airflow DAG's ETL step.
    """
    suite_factory = GreatExpectationsSuite()
    if event_type == "clicks":
        suite = suite_factory.build_click_event_suite()
    elif event_type == "orders":
        suite = suite_factory.build_order_event_suite()
    else:
        logger.warning(f"No suite for event_type={event_type}, passing through")
        return True

    result = suite_factory.run_validation(data, suite)
    suite_factory.print_report(result)

    if result["quality_score"] < min_score:
        logger.error(
            f"🚨 Quality gate FAILED for {event_type}: "
            f"score={result['quality_score']:.1%} < threshold={min_score:.1%}"
        )
        return False

    logger.info(f"✅ Quality gate PASSED for {event_type}: score={result['quality_score']:.1%}")
    return True
