"""Shared infrastructure for Google Ads MCP analytics tools.

- run_query: GAQL executor with error handling and quota tracking
- ClientResolver: MCC account name/ID mapping (24h cache)
- CampaignResolver: campaign name/ID mapping (1h cache)
- DateHelper: date math and GAQL date conditions
- QuotaTracker: daily API operation counter (15k Basic Access)
- ResultFormatter: markdown tables, currency, percentages
- compute_derived_metrics: spend, CPA, ROAS, CTR, CPC from raw API fields
- aggregate_rows: generic groupby aggregation for collapsing per-day rows
"""

import logging
import os
import threading
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import ads_mcp.utils as utils

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Query runner
# ---------------------------------------------------------------------------

def run_query(customer_id: str, query: str) -> List[Dict[str, Any]]:
    """Execute a GAQL query and return list of row dicts.

    Strips hyphens/prefixes from customer_id, tracks quota, catches errors.
    """
    customer_id = customer_id.replace("-", "").replace("customers/", "")
    QuotaTracker.increment()

    try:
        ga_service = utils.get_googleads_service("GoogleAdsService")
        logger.info("run_query cid=%s q=%s", customer_id, query[:120])
        result = ga_service.search_stream(customer_id=customer_id, query=query)
        rows: List[Dict[str, Any]] = []
        for batch in result:
            for row in batch.results:
                rows.append(utils.format_output_row(row, batch.field_mask.paths))
        return rows
    except Exception as e:
        error_msg = str(e)
        # Parse common Google Ads errors into readable messages
        if "CUSTOMER_NOT_FOUND" in error_msg:
            raise ValueError(f"Account {customer_id} not found. Check the customer ID.")
        if "PERMISSION_DENIED" in error_msg:
            raise ValueError(f"No access to account {customer_id}. Check MCC permissions.")
        if "QUERY_ERROR" in error_msg:
            raise ValueError(f"Invalid GAQL query: {error_msg[:200]}")
        raise ValueError(f"Google Ads API error: {error_msg[:300]}")


# ---------------------------------------------------------------------------
# Client Resolver
# ---------------------------------------------------------------------------

class ClientResolver:
    """Resolves client names to customer IDs under the MCC account."""

    _clients: Dict[str, str] = {}
    _clients_by_id: Dict[str, str] = {}
    _lock = threading.Lock()
    _last_refresh: Optional[datetime] = None
    _REFRESH_INTERVAL = timedelta(hours=24)

    @classmethod
    def _needs_refresh(cls) -> bool:
        if cls._last_refresh is None:
            return True
        return datetime.now() - cls._last_refresh > cls._REFRESH_INTERVAL

    @classmethod
    def refresh(cls) -> None:
        mcc_id = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").replace("-", "")
        if not mcc_id:
            logger.warning("GOOGLE_ADS_LOGIN_CUSTOMER_ID not set")
            return

        query = (
            "SELECT customer_client.client_customer, "
            "customer_client.descriptive_name, "
            "customer_client.status, "
            "customer_client.level "
            "FROM customer_client "
            "WHERE customer_client.level = 1"
        )

        try:
            rows = run_query(mcc_id, query)
        except Exception:
            logger.exception("ClientResolver.refresh failed")
            return

        with cls._lock:
            cls._clients.clear()
            cls._clients_by_id.clear()
            for row in rows:
                raw_cid = str(row.get("customer_client.client_customer", ""))
                cid = raw_cid.replace("customers/", "").replace("-", "")
                name = row.get("customer_client.descriptive_name", "")
                if cid and cid.isdigit():
                    cls._clients[name.lower()] = cid
                    cls._clients_by_id[cid] = name
            cls._last_refresh = datetime.now()
            logger.info("ClientResolver: %d clients loaded", len(cls._clients))

    @classmethod
    def ensure_loaded(cls) -> None:
        if cls._needs_refresh():
            cls.refresh()

    @classmethod
    def resolve(cls, client: str) -> str:
        cls.ensure_loaded()
        clean = client.replace("-", "").replace("customers/", "").strip()
        if clean.isdigit():
            return clean
        with cls._lock:
            cid = cls._clients.get(clean.lower())
            if cid:
                return cid
            for name, cid in cls._clients.items():
                if clean.lower() in name:
                    return cid
        available = ", ".join(f"{cls._clients_by_id[c]} ({c})" for c in cls._clients_by_id)
        raise ValueError(f"Client '{client}' not found. Available: {available}")

    @classmethod
    def resolve_name(cls, customer_id: str) -> str:
        """Return human name for a customer ID."""
        cls.ensure_loaded()
        clean = customer_id.replace("-", "").replace("customers/", "")
        with cls._lock:
            return cls._clients_by_id.get(clean, clean)

    @classmethod
    def get_all(cls) -> List[Dict[str, str]]:
        cls.ensure_loaded()
        with cls._lock:
            return [
                {"name": cls._clients_by_id.get(cid, ""), "id": cid}
                for cid in cls._clients_by_id
            ]


# ---------------------------------------------------------------------------
# Campaign Resolver
# ---------------------------------------------------------------------------

class CampaignResolver:
    _cache: Dict[str, Dict[str, str]] = {}
    _timestamps: Dict[str, datetime] = {}
    _lock = threading.Lock()
    _TTL = timedelta(hours=1)

    @classmethod
    def resolve(cls, customer_id: str, campaign: str) -> str:
        customer_id = customer_id.replace("-", "")
        clean = campaign.strip()
        if clean.isdigit():
            return clean
        cls._ensure_loaded(customer_id)
        with cls._lock:
            campaigns = cls._cache.get(customer_id, {})
            cid = campaigns.get(clean.lower())
            if cid:
                return cid
            for name, cid in campaigns.items():
                if clean.lower() in name:
                    return cid
        raise ValueError(f"Campaign '{campaign}' not found for customer {customer_id}.")

    @classmethod
    def _ensure_loaded(cls, customer_id: str) -> None:
        with cls._lock:
            ts = cls._timestamps.get(customer_id)
            if ts and datetime.now() - ts < cls._TTL:
                return
        query = "SELECT campaign.id, campaign.name FROM campaign ORDER BY campaign.name"
        rows = run_query(customer_id, query)
        with cls._lock:
            mapping: Dict[str, str] = {}
            for row in rows:
                cid = str(row.get("campaign.id", ""))
                name = row.get("campaign.name", "")
                if cid:
                    mapping[name.lower()] = cid
            cls._cache[customer_id] = mapping
            cls._timestamps[customer_id] = datetime.now()


# ---------------------------------------------------------------------------
# Asset Resolver (for PMax top combinations)
# ---------------------------------------------------------------------------

class AssetResolver:
    """Resolves asset resource names to readable content (1h cache)."""

    _cache: Dict[str, Dict[str, Dict[str, str]]] = {}
    _timestamps: Dict[str, datetime] = {}
    _lock = threading.Lock()
    _TTL = timedelta(hours=1)

    @classmethod
    def resolve(cls, customer_id: str) -> Dict[str, Dict[str, str]]:
        """Return dict keyed by asset resource_name → {name, text, image_url, video_id}."""
        customer_id = customer_id.replace("-", "")
        cls._ensure_loaded(customer_id)
        with cls._lock:
            return cls._cache.get(customer_id, {})

    @classmethod
    def _ensure_loaded(cls, customer_id: str) -> None:
        with cls._lock:
            ts = cls._timestamps.get(customer_id)
            if ts and datetime.now() - ts < cls._TTL:
                return
        query = (
            "SELECT asset.resource_name, asset.name, "
            "asset.text_asset.text, "
            "asset.image_asset.full_size.url, "
            "asset.youtube_video_asset.youtube_video_id "
            "FROM asset"
        )
        rows = run_query(customer_id, query)
        lookup: Dict[str, Dict[str, str]] = {}
        for row in rows:
            rn = row.get("asset.resource_name", "")
            if not rn:
                continue
            text = (
                row.get("asset.text_asset.text", "")
                or row.get("asset.image_asset.full_size.url", "")
                or row.get("asset.youtube_video_asset.youtube_video_id", "")
                or row.get("asset.name", "")
                or ""
            )
            lookup[rn] = {
                "name": row.get("asset.name", ""),
                "text": str(text),
                "image_url": row.get("asset.image_asset.full_size.url", "") or "",
                "video_id": row.get("asset.youtube_video_asset.youtube_video_id", "") or "",
            }
        with cls._lock:
            cls._cache[customer_id] = lookup
            cls._timestamps[customer_id] = datetime.now()
            logger.info("AssetResolver: %d assets loaded for %s", len(lookup), customer_id)


# ---------------------------------------------------------------------------
# Date Helper
# ---------------------------------------------------------------------------

class DateHelper:
    @staticmethod
    def parse_date(s: str) -> date:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()

    @staticmethod
    def previous_period(date_from: date, date_to: date) -> Tuple[date, date]:
        delta = date_to - date_from
        prev_to = date_from - timedelta(days=1)
        prev_from = prev_to - delta
        return prev_from, prev_to

    @staticmethod
    def format_date(d: date) -> str:
        return d.strftime("%Y-%m-%d")

    @staticmethod
    def date_condition(date_from: str, date_to: str) -> str:
        return f"segments.date BETWEEN '{date_from}' AND '{date_to}'"

    @staticmethod
    def days_ago(days: int) -> Tuple[str, str]:
        today = date.today()
        end = today - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        return DateHelper.format_date(start), DateHelper.format_date(end)


# ---------------------------------------------------------------------------
# Quota Tracker
# ---------------------------------------------------------------------------

class QuotaTracker:
    _count: int = 0
    _date: Optional[date] = None
    _lock = threading.Lock()
    _DAILY_LIMIT = 15000
    _WARNING = 12000
    _BLOCK = 14500

    @classmethod
    def increment(cls) -> None:
        with cls._lock:
            today = date.today()
            if cls._date != today:
                cls._count = 0
                cls._date = today
            cls._count += 1
            if cls._count >= cls._BLOCK:
                raise RuntimeError(
                    f"API quota exhausted: {cls._count}/{cls._DAILY_LIMIT}. Wait until tomorrow."
                )
            if cls._count >= cls._WARNING:
                logger.warning("Quota warning: %d/%d", cls._count, cls._DAILY_LIMIT)

    @classmethod
    def get_usage(cls) -> Dict[str, Any]:
        with cls._lock:
            today = date.today()
            if cls._date != today:
                return {"used": 0, "limit": cls._DAILY_LIMIT, "date": str(today)}
            return {"used": cls._count, "limit": cls._DAILY_LIMIT, "date": str(cls._date)}


# ---------------------------------------------------------------------------
# Result Formatter
# ---------------------------------------------------------------------------

class ResultFormatter:
    @staticmethod
    def markdown_table(
        rows: List[Dict[str, Any]],
        columns: List[Tuple[str, str]],
        max_rows: int = 50,
    ) -> str:
        if not rows:
            return "No data found."
        total = len(rows)
        display = rows[:max_rows]
        headers = [col[1] for col in columns]
        lines = ["| " + " | ".join(headers) + " |"]
        lines.append("| " + " | ".join("---" for _ in headers) + " |")
        for row in display:
            cells = [str(row.get(key, "")) for key, _ in columns]
            lines.append("| " + " | ".join(cells) + " |")
        if total > max_rows:
            lines.append(f"\n*Showing top {max_rows} of {total:,} results.*")
        return "\n".join(lines)

    @staticmethod
    def fmt_currency(v: float) -> str:
        return f"{v:,.2f}"

    @staticmethod
    def fmt_percent(v: float) -> str:
        return f"{v:,.1f}%"

    @staticmethod
    def fmt_int(v) -> str:
        return f"{int(v or 0):,}"

    @staticmethod
    def fmt_delta(current: float, previous: float) -> str:
        if previous == 0:
            return "0.0%" if current == 0 else "+∞"
        delta = ((current - previous) / previous) * 100
        sign = "+" if delta > 0 else ""
        return f"{sign}{delta:.1f}%"


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------

def compute_derived_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
    """Add _spend, _cpa, _roas, _ctr, _cpc to a row dict (in-place)."""
    cost_micros = float(row.get("metrics.cost_micros", 0) or 0)
    conversions = float(row.get("metrics.conversions", 0) or 0)
    conv_value = float(row.get("metrics.conversions_value", 0) or 0)
    clicks = int(row.get("metrics.clicks", 0) or 0)
    impressions = int(row.get("metrics.impressions", 0) or 0)

    spend = cost_micros / 1_000_000
    row["_spend"] = round(spend, 2)
    row["_cpa"] = round(spend / conversions, 2) if conversions > 0 else 0.0
    row["_roas"] = round(conv_value / spend, 2) if spend > 0 else 0.0
    row["_ctr"] = round(clicks / impressions * 100, 2) if impressions > 0 else 0.0
    row["_cpc"] = round(spend / clicks, 2) if clicks > 0 else 0.0
    return row


# ---------------------------------------------------------------------------
# Generic aggregation helper
# ---------------------------------------------------------------------------

SUMMABLE_METRICS = {
    "metrics.impressions", "metrics.clicks", "metrics.cost_micros",
    "metrics.conversions", "metrics.conversions_value",
    "metrics.search_impression_share",
}

def aggregate_rows(
    rows: List[Dict[str, Any]],
    group_by: List[str],
    sum_fields: Optional[Set[str]] = None,
    collect_fields: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Aggregate rows by group_by keys, summing metrics and collecting sets.

    Args:
        rows: raw API rows
        group_by: fields to group by (e.g. ["search_term_view.search_term"])
        sum_fields: fields to sum (defaults to SUMMABLE_METRICS)
        collect_fields: {field: label} — fields to collect as sets,
            output as "N {label}" if >1, else the single value
    Returns:
        Aggregated rows with summed metrics and collected fields
    """
    if sum_fields is None:
        sum_fields = SUMMABLE_METRICS
    if collect_fields is None:
        collect_fields = {}

    agg: Dict[tuple, Dict[str, Any]] = defaultdict(lambda: {})

    for row in rows:
        key = tuple(row.get(f, "") for f in group_by)
        bucket = agg[key]

        # Initialize on first row
        if not bucket:
            for f in group_by:
                bucket[f] = row.get(f, "")
            for f in sum_fields:
                bucket[f] = 0.0
            for f in collect_fields:
                bucket[f"_set_{f}"] = set()

        # Sum metrics
        for f in sum_fields:
            val = row.get(f, 0)
            bucket[f] += float(val) if val else 0.0

        # Collect sets
        for f in collect_fields:
            val = row.get(f, "")
            if val:
                bucket[f"_set_{f}"].add(val)

    # Finalize
    result = []
    for bucket in agg.values():
        for f, label in collect_fields.items():
            s = bucket.pop(f"_set_{f}")
            bucket[f] = f"{len(s)} {label}" if len(s) > 1 else next(iter(s), "")
        # Cast back to int where appropriate
        for f in ("metrics.impressions", "metrics.clicks"):
            if f in bucket:
                bucket[f] = int(bucket[f])
        result.append(bucket)

    return result
