"""Shared infrastructure for custom Google Ads analytics tools.

Provides:
- ClientResolver: MCC → child account name/ID mapping
- CampaignResolver: campaign name → ID resolution with cache
- DateHelper: human-readable dates → GAQL date ranges
- QuotaTracker: in-memory API operation counter
- ResultFormatter: markdown table formatting
- run_query: thin wrapper around search_stream
"""

import logging
import os
import threading
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import ads_mcp.utils as utils

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Query runner
# ---------------------------------------------------------------------------

def run_query(customer_id: str, query: str) -> List[Dict[str, Any]]:
    """Execute a GAQL query via search_stream and return list of row dicts.

    Automatically strips hyphens from customer_id and tracks quota.
    """
    customer_id = customer_id.replace("-", "")
    QuotaTracker.increment()

    ga_service = utils.get_googleads_service("GoogleAdsService")
    logger.info("run_query customer_id=%s query=%s", customer_id, query)

    result = ga_service.search_stream(customer_id=customer_id, query=query)

    rows: List[Dict[str, Any]] = []
    for batch in result:
        for row in batch.results:
            rows.append(utils.format_output_row(row, batch.field_mask.paths))
    return rows


# ---------------------------------------------------------------------------
# Client Resolver
# ---------------------------------------------------------------------------

class ClientResolver:
    """Resolves client names to customer IDs under the MCC account."""

    _clients: Dict[str, str] = {}  # descriptive_name_lower -> customer_id
    _clients_by_id: Dict[str, str] = {}  # customer_id -> descriptive_name
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
        """Query the MCC for child accounts and build the lookup maps."""
        mcc_id = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").replace("-", "")
        if not mcc_id:
            logger.warning("GOOGLE_ADS_LOGIN_CUSTOMER_ID not set; ClientResolver disabled")
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
                if cid:
                    cls._clients[name.lower()] = cid
                    cls._clients_by_id[cid] = name
            cls._last_refresh = datetime.now()
            logger.info("ClientResolver refreshed: %d clients", len(cls._clients))

    @classmethod
    def ensure_loaded(cls) -> None:
        if cls._needs_refresh():
            cls.refresh()

    @classmethod
    def resolve(cls, client: str) -> str:
        """Accept a client name or ID and return the numeric customer ID."""
        cls.ensure_loaded()
        clean = client.replace("-", "").strip()
        # If it's all digits, assume it's already an ID
        if clean.isdigit():
            return clean
        # Try exact match on lowercase name
        with cls._lock:
            cid = cls._clients.get(clean.lower())
            if cid:
                return cid
            # Try partial match
            for name, cid in cls._clients.items():
                if clean.lower() in name:
                    return cid
        raise ValueError(
            f"Client '{client}' not found. Available: "
            + ", ".join(f"{v} ({k})" for k, v in cls._clients.items())
        )

    @classmethod
    def get_all(cls) -> List[Dict[str, str]]:
        """Return list of all clients with name, id, status info."""
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
    """Lazy-loads campaign name → ID mapping per client with 1h TTL."""

    _cache: Dict[str, Dict[str, str]] = {}  # customer_id -> {name_lower: campaign_id}
    _timestamps: Dict[str, datetime] = {}
    _lock = threading.Lock()
    _TTL = timedelta(hours=1)

    @classmethod
    def resolve(cls, customer_id: str, campaign: str) -> str:
        """Resolve campaign name or ID to numeric campaign ID."""
        customer_id = customer_id.replace("-", "")
        clean = campaign.replace("-", "").strip()
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
        raise ValueError(
            f"Campaign '{campaign}' not found for customer {customer_id}."
        )

    @classmethod
    def _ensure_loaded(cls, customer_id: str) -> None:
        with cls._lock:
            ts = cls._timestamps.get(customer_id)
            if ts and datetime.now() - ts < cls._TTL:
                return

        query = (
            "SELECT campaign.id, campaign.name "
            "FROM campaign "
            "ORDER BY campaign.name"
        )
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
# Date Helper
# ---------------------------------------------------------------------------

class DateHelper:
    """Convert human-readable date strings to GAQL date ranges."""

    @staticmethod
    def parse_date(s: str) -> date:
        """Parse a date string in YYYY-MM-DD format."""
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()

    @staticmethod
    def previous_period(date_from: date, date_to: date) -> Tuple[date, date]:
        """Calculate the previous period of the same duration."""
        delta = date_to - date_from
        prev_to = date_from - timedelta(days=1)
        prev_from = prev_to - delta
        return prev_from, prev_to

    @staticmethod
    def format_date(d: date) -> str:
        return d.strftime("%Y-%m-%d")

    @staticmethod
    def date_condition(date_from: str, date_to: str) -> str:
        """Return a GAQL WHERE clause fragment for a date range."""
        return f"segments.date BETWEEN '{date_from}' AND '{date_to}'"

    @staticmethod
    def days_ago(days: int) -> Tuple[str, str]:
        """Return (from, to) strings for the last N days ending yesterday."""
        today = date.today()
        end = today - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        return DateHelper.format_date(start), DateHelper.format_date(end)


# ---------------------------------------------------------------------------
# Quota Tracker
# ---------------------------------------------------------------------------

class QuotaTracker:
    """In-memory counter for daily Google Ads API operations."""

    _count: int = 0
    _date: Optional[date] = None
    _lock = threading.Lock()
    _DAILY_LIMIT = 15000
    _WARNING_THRESHOLD = 12000   # 80%
    _SOFT_BLOCK_THRESHOLD = 14500  # 97%

    @classmethod
    def increment(cls) -> None:
        with cls._lock:
            today = date.today()
            if cls._date != today:
                cls._count = 0
                cls._date = today
            cls._count += 1
            if cls._count >= cls._SOFT_BLOCK_THRESHOLD:
                raise RuntimeError(
                    f"API quota soft-block: {cls._count}/{cls._DAILY_LIMIT} operations used today. "
                    "Please wait until tomorrow."
                )
            if cls._count >= cls._WARNING_THRESHOLD:
                logger.warning(
                    "API quota warning: %d/%d operations used today",
                    cls._count, cls._DAILY_LIMIT,
                )

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
    """Format query results as markdown tables."""

    @staticmethod
    def markdown_table(
        rows: List[Dict[str, Any]],
        columns: List[Tuple[str, str]],
        max_rows: int = 50,
    ) -> str:
        """Build a markdown table from rows.

        Args:
            rows: list of dicts with data
            columns: list of (key, header_label) tuples
            max_rows: truncate after this many rows
        """
        if not rows:
            return "No data found."

        total = len(rows)
        display_rows = rows[:max_rows]

        # Header
        headers = [col[1] for col in columns]
        lines = ["| " + " | ".join(headers) + " |"]
        lines.append("| " + " | ".join("---" for _ in headers) + " |")

        # Rows
        for row in display_rows:
            cells = []
            for key, _ in columns:
                val = row.get(key, "")
                cells.append(str(val))
            lines.append("| " + " | ".join(cells) + " |")

        if total > max_rows:
            lines.append(f"\n*Showing top {max_rows} of {total:,} results.*")

        return "\n".join(lines)

    @staticmethod
    def format_currency(value: float) -> str:
        """Format a float as currency with 2 decimals."""
        return f"{value:,.2f}"

    @staticmethod
    def format_percent(value: float) -> str:
        """Format a float as percentage with 1 decimal."""
        return f"{value:,.1f}%"

    @staticmethod
    def format_delta(current: float, previous: float) -> str:
        """Calculate and format percentage change."""
        if previous == 0:
            if current == 0:
                return "0.0%"
            return "+∞"
        delta = ((current - previous) / previous) * 100
        sign = "+" if delta > 0 else ""
        return f"{sign}{delta:.1f}%"


# ---------------------------------------------------------------------------
# Metric computation helpers
# ---------------------------------------------------------------------------

def compute_derived_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
    """Add computed spend, CPA, ROAS to a row dict (in-place and returned)."""
    cost_micros = float(row.get("metrics.cost_micros", 0) or 0)
    conversions = float(row.get("metrics.conversions", 0) or 0)
    conv_value = float(row.get("metrics.conversions_value", 0) or 0)

    spend = cost_micros / 1_000_000
    cpa = spend / conversions if conversions > 0 else 0.0
    roas = conv_value / spend if spend > 0 else 0.0

    row["_spend"] = round(spend, 2)
    row["_cpa"] = round(cpa, 2)
    row["_roas"] = round(roas, 2)
    return row
