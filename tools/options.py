"""Shared options module for all Google Ads MCP tools.

Centralizes ALL cross-cutting logic so adding a new feature means
changing ONE file, not eleven. Every tool imports from here.

Architecture:
    GAQL query → raw rows → aggregate_rows() → compute_derived_metrics()
                          → apply_filters()  → apply_sort()
                          → apply_limit()    → format_output()

Sections:
    1. TEXT FILTERS       - contains/excludes on any text field
    2. NUMERIC FILTERS    - min/max thresholds on metrics
    3. COMBINED FILTER    - single entry point for all filtering
    4. SORTING            - unified sort by any metric
    5. LIMIT & TRUNCATION - limit + "showing X of Y" message
    6. OUTPUT FORMATTING  - markdown table, CSV, header/footer builders
    7. PERIOD COMPARISON  - delta calculation and formatting
    8. BENCHMARKS & FLAGS - proactive alerts on unhealthy metrics
    9. COLUMN PRESETS     - predefined column sets per entity type
    10. SEGMENTATION      - GAQL segment helpers (device, geo, network, etc.)

Usage in any tool:
    from tools.options import (
        apply_filters, apply_sort, apply_limit,
        format_output, build_header, build_footer,
        COLUMNS, Benchmarks
    )
"""

import csv
import io
import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ===========================================================================
# 1. TEXT FILTERS
# ===========================================================================

def _parse_csv(s: str) -> List[str]:
    """Split comma-separated string into lowercase stripped tokens."""
    return [w.strip().lower() for w in s.split(",") if w.strip()]


def text_match(value: str, contains: str = "", excludes: str = "") -> bool:
    """Check if a text value passes contains/excludes filters.

    contains: comma-separated — value must contain ANY of these words
    excludes: comma-separated — value must NOT contain ANY of these words
    Both are case-insensitive substring matches.
    """
    val_lower = value.lower()

    if contains:
        include_words = _parse_csv(contains)
        if include_words and not any(w in val_lower for w in include_words):
            return False

    if excludes:
        exclude_words = _parse_csv(excludes)
        if exclude_words and any(w in val_lower for w in exclude_words):
            return False

    return True


# ===========================================================================
# 2. NUMERIC FILTERS
# ===========================================================================

def numeric_match(
    row: Dict[str, Any],
    min_clicks: int = 0,
    min_impressions: int = 0,
    min_conversions: float = 0,
    max_cpa: float = 0,
    min_roas: float = 0,
    min_ctr: float = 0,
    max_cpc: float = 0,
    min_spend: float = 0,
    max_spend: float = 0,
    zero_conversions: bool = False,
) -> bool:
    """Check if a row passes all numeric filters.

    All thresholds at 0 (or 0.0) mean "no filter".
    zero_conversions=True: keep ONLY rows with 0 conversions (waste finder).

    Expects row processed by compute_derived_metrics() so
    _spend, _cpa, _roas, _ctr, _cpc exist.
    """
    clicks = int(row.get("metrics.clicks", 0) or 0)
    impressions = int(row.get("metrics.impressions", 0) or 0)
    conv = float(row.get("metrics.conversions", 0) or 0)
    cpa = float(row.get("_cpa", 0) or 0)
    roas = float(row.get("_roas", 0) or 0)
    ctr = float(row.get("_ctr", 0) or 0)
    cpc = float(row.get("_cpc", 0) or 0)
    spend = float(row.get("_spend", 0) or 0)

    if clicks < min_clicks:
        return False
    if impressions < min_impressions:
        return False

    if zero_conversions:
        if conv > 0:
            return False
    else:
        if min_conversions > 0 and conv < min_conversions:
            return False
        if max_cpa > 0 and cpa > max_cpa and conv > 0:
            return False
        if min_roas > 0 and roas < min_roas and conv > 0:
            return False

    if min_ctr > 0 and ctr < min_ctr:
        return False
    if max_cpc > 0 and cpc > max_cpc:
        return False
    if min_spend > 0 and spend < min_spend:
        return False
    if max_spend > 0 and spend > max_spend:
        return False

    return True


# ===========================================================================
# 3. COMBINED FILTER (single entry point)
# ===========================================================================

def apply_filters(
    rows: List[Dict[str, Any]],
    text_field: str = "",
    contains: str = "",
    excludes: str = "",
    min_clicks: int = 0,
    min_impressions: int = 0,
    min_conversions: float = 0,
    max_cpa: float = 0,
    min_roas: float = 0,
    min_ctr: float = 0,
    max_cpc: float = 0,
    min_spend: float = 0,
    max_spend: float = 0,
    zero_conversions: bool = False,
    status: str = "",
    campaign_type: str = "",
) -> List[Dict[str, Any]]:
    """Apply ALL filters to a list of rows (post-aggregation).

    Args:
        rows: list of dicts (post compute_derived_metrics)
        text_field: key to apply text filters on
            ("term", "campaign.name", "kw_text", "ngram", "ad_group.name")
        contains/excludes: comma-separated text filters
        min_*/max_*: numeric thresholds (0 = disabled)
        zero_conversions: if True, keep ONLY zero-conversion rows
        status: filter by entity status ("ENABLED", "PAUSED", etc.)
        campaign_type: filter by campaign channel type ("SEARCH", "SHOPPING", etc.)

    Returns:
        Filtered list of rows
    """
    result = []
    status_lower = status.lower().strip() if status else ""
    ctype_lower = campaign_type.lower().strip() if campaign_type else ""

    for row in rows:
        # --- Text filter ---
        if text_field and (contains or excludes):
            text_value = str(row.get(text_field, ""))
            if not text_match(text_value, contains, excludes):
                continue

        # --- Status filter ---
        if status_lower:
            row_status = str(
                row.get("campaign.status", "")
                or row.get("ad_group.status", "")
                or row.get("ad_group_criterion.status", "")
            ).lower()
            if status_lower not in row_status:
                continue

        # --- Campaign type filter ---
        if ctype_lower:
            row_type = str(
                row.get("campaign.advertising_channel_type", "")
            ).lower()
            if ctype_lower not in row_type:
                continue

        # --- Numeric filters ---
        if not numeric_match(
            row,
            min_clicks=min_clicks,
            min_impressions=min_impressions,
            min_conversions=min_conversions,
            max_cpa=max_cpa,
            min_roas=min_roas,
            min_ctr=min_ctr,
            max_cpc=max_cpc,
            min_spend=min_spend,
            max_spend=max_spend,
            zero_conversions=zero_conversions,
        ):
            continue

        result.append(row)

    return result


# ===========================================================================
# 4. SORTING
# ===========================================================================

SORT_KEYS = {
    "spend":        "_spend",
    "clicks":       "metrics.clicks",
    "impressions":  "metrics.impressions",
    "conversions":  "metrics.conversions",
    "conv_value":   "metrics.conversions_value",
    "cpa":          "_cpa",
    "roas":         "_roas",
    "ctr":          "_ctr",
    "cpc":          "_cpc",
    "quality_score": "qs",
    "search_is":    "search_is",
    "budget_lost":  "budget_lost_is",
    "rank_lost":    "rank_lost_is",
}


def apply_sort(
    rows: List[Dict[str, Any]],
    sort_by: str = "spend",
    ascending: bool = False,
) -> List[Dict[str, Any]]:
    """Sort rows by a metric name.

    Args:
        sort_by: user-facing metric name (spend, clicks, cpa, roas, etc.)
        ascending: False = highest first (default for spend/clicks)
                   True = lowest first (useful for CPA)
    """
    sort_key = SORT_KEYS.get(sort_by.lower(), "_spend")
    return sorted(
        rows,
        key=lambda r: float(r.get(sort_key, 0) or 0),
        reverse=not ascending,
    )


# ===========================================================================
# 5. LIMIT & TRUNCATION
# ===========================================================================

def apply_limit(
    rows: List[Dict[str, Any]],
    limit: int = 50,
) -> Tuple[List[Dict[str, Any]], int, bool]:
    """Apply row limit and return (limited_rows, total_count, was_truncated).

    Returns:
        (rows[:limit], len(rows), True/False)
    """
    total = len(rows)
    truncated = total > limit
    return rows[:limit], total, truncated


# ===========================================================================
# 6. OUTPUT FORMATTING
# ===========================================================================

class OutputFormat:
    """Generate different output formats from processed rows."""

    @staticmethod
    def markdown_table(
        rows: List[Dict[str, Any]],
        columns: List[Tuple[str, str]],
    ) -> str:
        """Build a markdown table.

        Args:
            rows: data rows
            columns: list of (key, header_label) tuples
        """
        if not rows:
            return "No data found."

        headers = [col[1] for col in columns]
        lines = ["| " + " | ".join(headers) + " |"]
        lines.append("| " + " | ".join("---" for _ in headers) + " |")

        for row in rows:
            cells = []
            for key, _ in columns:
                val = row.get(key, "")
                cells.append(str(val) if val is not None else "")
            lines.append("| " + " | ".join(cells) + " |")

        return "\n".join(lines)

    @staticmethod
    def csv_string(
        rows: List[Dict[str, Any]],
        columns: List[Tuple[str, str]],
    ) -> str:
        """Build a CSV string (for future Sheets export)."""
        if not rows:
            return ""
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([col[1] for col in columns])
        for row in rows:
            writer.writerow([str(row.get(key, "")) for key, _ in columns])
        return output.getvalue()

    @staticmethod
    def summary_row(
        rows: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Compute a TOTAL summary row across all rows."""
        if not rows:
            return {}

        total = {
            "metrics.impressions": 0,
            "metrics.clicks": 0,
            "metrics.conversions": 0.0,
            "metrics.conversions_value": 0.0,
            "metrics.cost_micros": 0,
        }
        for row in rows:
            total["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
            total["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
            total["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
            total["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)
            total["metrics.cost_micros"] += int(row.get("metrics.cost_micros", 0) or 0)

        # Derive totals
        spend = total["metrics.cost_micros"] / 1_000_000
        conv = total["metrics.conversions"]
        conv_val = total["metrics.conversions_value"]
        clicks = total["metrics.clicks"]
        impr = total["metrics.impressions"]

        total["_spend"] = round(spend, 2)
        total["_cpa"] = round(spend / conv, 2) if conv > 0 else 0.0
        total["_roas"] = round(conv_val / spend, 2) if spend > 0 else 0.0
        total["_ctr"] = round((clicks / impr) * 100, 2) if impr > 0 else 0.0
        total["_cpc"] = round(spend / clicks, 2) if clicks > 0 else 0.0

        return total


def format_output(
    rows: List[Dict[str, Any]],
    columns: List[Tuple[str, str]],
    header: str = "",
    footer: str = "",
    output_format: str = "markdown",
    output_mode: str = "summary",
    all_rows: Optional[List[Dict[str, Any]]] = None,
    summary_limit: int = 10,
) -> str:
    """Full output pipeline: header + table + footer.

    Args:
        output_format: "markdown" (default) or "csv"
        output_mode: "summary" (default) or "full"
            - summary: shows top N rows + totals + "N more available"
            - full: shows all rows passed in
        all_rows: complete filtered+sorted dataset BEFORE limit (for summary totals).
            If None, totals are computed from rows.
        summary_limit: how many rows to show in summary mode (default 10)
    """
    parts = []

    if header:
        parts.append(header)

    if output_mode == "summary" and len(rows) > summary_limit:
        # Summary mode: show top N + totals over ALL data
        display = rows[:summary_limit]
        data_for_totals = all_rows if all_rows is not None else rows
        summary = OutputFormat.summary_row(data_for_totals)

        if output_format == "csv":
            parts.append(OutputFormat.csv_string(display, columns))
        else:
            parts.append(OutputFormat.markdown_table(display, columns))

        # Summary totals
        remaining = len(rows) - summary_limit
        total_data = len(data_for_totals)
        summary_parts = []
        if summary:
            spend = summary.get("_spend", 0)
            conv = summary.get("metrics.conversions", 0)
            roas = summary.get("_roas", 0)
            cpa = summary.get("_cpa", 0)
            clicks = summary.get("metrics.clicks", 0)
            summary_parts.append(
                f"**Totals ({total_data:,} rows)**: "
                f"Spend €{spend:,.2f} · Clicks {clicks:,} · "
                f"Conv {conv:,.1f} · CPA €{cpa:,.2f} · ROAS {roas:.2f}x"
            )
        summary_parts.append(
            f"*Showing top {summary_limit} of {len(rows):,} results. "
            f"Ask for full data or increase limit to see more.*"
        )
        parts.append("\n".join(summary_parts))
    else:
        # Full mode or small dataset: show everything
        if output_format == "csv":
            parts.append(OutputFormat.csv_string(rows, columns))
        else:
            parts.append(OutputFormat.markdown_table(rows, columns))

        if footer:
            parts.append(footer)

    return "\n\n".join(parts)


# ===========================================================================
# 6b. HEADER & FOOTER BUILDERS
# ===========================================================================

def build_header(
    title: str,
    client_name: str = "",
    date_from: str = "",
    date_to: str = "",
    filter_desc: str = "",
    extra: str = "",
) -> str:
    """Build a standardized output header."""
    parts = [f"**{title}**"]
    if client_name:
        parts.append(f"Account: {client_name}")
    if date_from and date_to:
        parts.append(f"Period: {date_from} → {date_to}")
    if filter_desc:
        parts.append(f"Filters: {filter_desc}")
    if extra:
        parts.append(extra)
    return " · ".join(parts)


def build_filter_description(
    contains: str = "",
    excludes: str = "",
    min_clicks: int = 0,
    min_impressions: int = 0,
    min_conversions: float = 0,
    max_cpa: float = 0,
    min_roas: float = 0,
    min_ctr: float = 0,
    max_cpc: float = 0,
    min_spend: float = 0,
    max_spend: float = 0,
    zero_conversions: bool = False,
    status: str = "",
    campaign_type: str = "",
) -> str:
    """Build human-readable string of active filters."""
    parts = []
    if contains:
        parts.append(f'contains: "{contains}"')
    if excludes:
        parts.append(f'excludes: "{excludes}"')
    if status:
        parts.append(f"status: {status}")
    if campaign_type:
        parts.append(f"type: {campaign_type}")
    if min_clicks > 0:
        parts.append(f"clicks ≥ {min_clicks}")
    if min_impressions > 0:
        parts.append(f"impressions ≥ {min_impressions}")
    if min_conversions > 0:
        parts.append(f"conv ≥ {min_conversions}")
    if max_cpa > 0:
        parts.append(f"CPA ≤ {max_cpa}")
    if min_roas > 0:
        parts.append(f"ROAS ≥ {min_roas}")
    if min_ctr > 0:
        parts.append(f"CTR ≥ {min_ctr}%")
    if max_cpc > 0:
        parts.append(f"CPC ≤ {max_cpc}")
    if min_spend > 0:
        parts.append(f"spend ≥ {min_spend}")
    if max_spend > 0:
        parts.append(f"spend ≤ {max_spend}")
    if zero_conversions:
        parts.append("zero conversions only")

    return "; ".join(parts) if parts else ""


def build_footer(
    total: int,
    shown: int,
    truncated: bool,
    summary: Optional[Dict[str, Any]] = None,
) -> str:
    """Build standardized footer with row count and optional summary."""
    parts = []
    if truncated:
        parts.append(f"*Showing {shown:,} of {total:,} results.*")
    else:
        parts.append(f"*{total:,} results.*")

    if summary:
        spend = summary.get("_spend", 0)
        conv = summary.get("metrics.conversions", 0)
        roas = summary.get("_roas", 0)
        cpa = summary.get("_cpa", 0)
        parts.append(
            f"**Totals**: Spend €{spend:,.2f} · "
            f"Conv {conv:,.1f} · CPA €{cpa:,.2f} · ROAS {roas:.2f}x"
        )

    return "\n".join(parts)


# ===========================================================================
# 7. PERIOD COMPARISON
# ===========================================================================

class PeriodComparison:
    """Helpers for comparing current vs previous period."""

    @staticmethod
    def calculate_previous_period(
        date_from: str, date_to: str
    ) -> Tuple[str, str]:
        """Given a date range, compute the equivalent previous period.

        Example: 2025-01-15 to 2025-01-31 (17 days)
                 → prev: 2024-12-28 to 2025-01-14
        """
        d_from = _parse_date(date_from)
        d_to = _parse_date(date_to)
        delta = d_to - d_from
        prev_to = d_from - timedelta(days=1)
        prev_from = prev_to - delta
        return prev_from.strftime("%Y-%m-%d"), prev_to.strftime("%Y-%m-%d")

    @staticmethod
    def compute_deltas(
        current: Dict[str, Any],
        previous: Dict[str, Any],
        metrics: List[str],
    ) -> Dict[str, str]:
        """Compute percentage change for a list of metric keys.

        Returns dict of {metric_key: "+12.3%" or "-5.1%"}
        """
        deltas = {}
        for m in metrics:
            curr_val = float(current.get(m, 0) or 0)
            prev_val = float(previous.get(m, 0) or 0)
            deltas[m] = _format_delta(curr_val, prev_val)
        return deltas

    @staticmethod
    def format_comparison_row(
        label: str,
        current_val: float,
        previous_val: float,
        fmt: str = "number",
    ) -> str:
        """Format a single comparison line.

        fmt: "number", "currency", "percent", "multiplier"
        """
        delta = _format_delta(current_val, previous_val)
        if fmt == "currency":
            return f"{label}: €{current_val:,.2f} (was €{previous_val:,.2f}, {delta})"
        elif fmt == "percent":
            return f"{label}: {current_val:.1f}% (was {previous_val:.1f}%, {delta})"
        elif fmt == "multiplier":
            return f"{label}: {current_val:.2f}x (was {previous_val:.2f}x, {delta})"
        else:
            return f"{label}: {current_val:,.0f} (was {previous_val:,.0f}, {delta})"


# ===========================================================================
# 8. BENCHMARKS & PROACTIVE FLAGS
# ===========================================================================

class Benchmarks:
    """E-commerce Google Ads benchmarks for proactive alerting."""

    # --- Thresholds ---
    CPA_WARNING = 50.0        # € — above this is a problem
    CPA_GOOD = 15.0           # € — below this is strong
    CTR_LOW = 2.0             # % — below this needs attention
    CTR_GOOD = 5.0            # % — above this is strong
    ROAS_WARNING = 3.0        # x — below this needs attention
    ROAS_GOOD = 5.0           # x — above this is strong
    QS_LOW = 5                # below 5 needs attention
    QS_GOOD = 7               # 7+ is healthy
    IS_LOW = 50.0             # % — below 50% is a big gap
    IS_GOOD = 80.0            # % — above 80% is strong
    BUDGET_LOST_WARNING = 20.0  # % — losing >20% IS due to budget
    RANK_LOST_WARNING = 20.0    # % — losing >20% IS due to rank

    @classmethod
    def flag_row(cls, row: Dict[str, Any]) -> List[str]:
        """Generate warning flags for a single row.

        Returns list of emoji+text warnings, empty if all healthy.
        """
        flags = []

        # CPA
        cpa = float(row.get("_cpa", 0) or 0)
        conv = float(row.get("metrics.conversions", 0) or 0)
        if cpa > cls.CPA_WARNING and conv > 0:
            flags.append(f"⚠️ CPA €{cpa:.0f} (>{cls.CPA_WARNING})")

        # CTR
        ctr = float(row.get("_ctr", 0) or 0)
        clicks = int(row.get("metrics.clicks", 0) or 0)
        if ctr < cls.CTR_LOW and clicks > 0:
            flags.append(f"⚠️ CTR {ctr:.1f}% (<{cls.CTR_LOW}%)")

        # ROAS
        roas = float(row.get("_roas", 0) or 0)
        spend = float(row.get("_spend", 0) or 0)
        if roas < cls.ROAS_WARNING and spend > 50:
            flags.append(f"⚠️ ROAS {roas:.1f}x (<{cls.ROAS_WARNING}x)")

        # Quality Score
        qs = int(row.get("qs", 0) or 0)
        if 0 < qs < cls.QS_LOW:
            flags.append(f"⚠️ QS {qs} (<{cls.QS_LOW})")

        # Impression Share
        search_is = float(row.get("search_is", 0) or 0)
        if 0 < search_is < cls.IS_LOW:
            flags.append(f"⚠️ IS {search_is:.0f}% (<{cls.IS_LOW}%)")

        # Budget Lost IS
        budget_lost = float(row.get("budget_lost_is", 0) or 0)
        if budget_lost > cls.BUDGET_LOST_WARNING:
            flags.append(f"🔴 Budget Lost IS {budget_lost:.0f}%")

        # Rank Lost IS
        rank_lost = float(row.get("rank_lost_is", 0) or 0)
        if rank_lost > cls.RANK_LOST_WARNING:
            flags.append(f"🟡 Rank Lost IS {rank_lost:.0f}%")

        # Zero conversions with significant spend
        if conv == 0 and spend > 100:
            flags.append(f"🔴 €{spend:.0f} spent, 0 conversions")

        return flags

    @classmethod
    def summarize_flags(
        cls, rows: List[Dict[str, Any]], name_field: str = ""
    ) -> str:
        """Scan all rows and return a summary of flagged entities."""
        flagged = []
        for row in rows:
            flags = cls.flag_row(row)
            if flags:
                name = row.get(name_field, "Unknown") if name_field else "Row"
                flagged.append(f"**{name}**: " + " · ".join(flags))

        if not flagged:
            return ""
        return "**⚠️ Alerts:**\n" + "\n".join(flagged)


# ===========================================================================
# 9. COLUMN PRESETS
# ===========================================================================

class COLUMNS:
    """Predefined column sets per entity type.

    Each is a list of (key, header_label) tuples for OutputFormat.
    Tools can extend or override these.
    """

    # --- Core metrics (shared by all) ---
    CORE_METRICS = [
        ("_spend", "Spend €"),
        ("metrics.clicks", "Clicks"),
        ("metrics.impressions", "Impr"),
        ("_ctr", "CTR%"),
        ("_cpc", "CPC €"),
        ("metrics.conversions", "Conv"),
        ("_cpa", "CPA €"),
        ("metrics.conversions_value", "Value €"),
        ("_roas", "ROAS"),
    ]

    # --- Campaign level ---
    CAMPAIGN = [
        ("campaign.name", "Campaign"),
        ("campaign.status", "Status"),
        ("campaign.advertising_channel_type", "Type"),
    ] + CORE_METRICS + [
        ("search_is", "IS%"),
        ("budget_lost_is", "Budget Lost%"),
        ("rank_lost_is", "Rank Lost%"),
    ]

    CAMPAIGN_COMPACT = [
        ("campaign.name", "Campaign"),
    ] + CORE_METRICS

    # --- Ad Group level ---
    ADGROUP = [
        ("campaign.name", "Campaign"),
        ("ad_group.name", "Ad Group"),
        ("ad_group.status", "Status"),
    ] + CORE_METRICS

    ADGROUP_COMPACT = [
        ("ad_group.name", "Ad Group"),
    ] + CORE_METRICS

    # --- Keyword level ---
    KEYWORD = [
        ("kw_text", "Keyword"),
        ("kw_match_type", "Match"),
        ("campaign.name", "Campaign"),
        ("ad_group.name", "Ad Group"),
    ] + CORE_METRICS + [
        ("qs", "QS"),
    ]

    KEYWORD_COMPACT = [
        ("kw_text", "Keyword"),
        ("kw_match_type", "Match"),
    ] + CORE_METRICS + [
        ("qs", "QS"),
    ]

    # --- Search Term level ---
    SEARCH_TERM = [
        ("term", "Search Term"),
    ] + CORE_METRICS

    SEARCH_TERM_DETAIL = [
        ("term", "Search Term"),
        ("campaign.name", "Campaign"),
        ("ad_group.name", "Ad Group"),
    ] + CORE_METRICS

    # --- N-Gram level ---
    NGRAM = [
        ("ngram", "N-Gram"),
        ("term_count", "Terms"),
    ] + CORE_METRICS

    # --- Change History ---
    CHANGE_HISTORY = [
        ("date", "Date"),
        ("change_type", "Change"),
        ("entity_type", "Entity"),
        ("entity_name", "Name"),
        ("old_value", "Old"),
        ("new_value", "New"),
        ("user_email", "Changed By"),
    ]

    # --- Conversion Setup ---
    CONVERSION_SETUP = [
        ("name", "Conversion Action"),
        ("category", "Category"),
        ("status", "Status"),
        ("attribution_model", "Attribution"),
        ("lookback_window_days", "Lookback"),
        ("include_in_conversions", "In Conv?"),
        ("counting_type", "Counting"),
    ]

    @classmethod
    def get(cls, entity: str, compact: bool = False) -> List[Tuple[str, str]]:
        """Get column preset by entity name.

        entity: "campaign", "adgroup", "keyword", "search_term",
                "ngram", "change_history", "conversion_setup"
        compact: if True, return compact version (fewer columns)
        """
        presets = {
            "campaign":          (cls.CAMPAIGN, cls.CAMPAIGN_COMPACT),
            "adgroup":           (cls.ADGROUP, cls.ADGROUP_COMPACT),
            "keyword":           (cls.KEYWORD, cls.KEYWORD_COMPACT),
            "search_term":       (cls.SEARCH_TERM, cls.SEARCH_TERM),
            "search_term_detail": (cls.SEARCH_TERM_DETAIL, cls.SEARCH_TERM_DETAIL),
            "ngram":             (cls.NGRAM, cls.NGRAM),
            "change_history":    (cls.CHANGE_HISTORY, cls.CHANGE_HISTORY),
            "conversion_setup":  (cls.CONVERSION_SETUP, cls.CONVERSION_SETUP),
        }
        full, compact_ver = presets.get(entity, (cls.CORE_METRICS, cls.CORE_METRICS))
        return compact_ver if compact else full


# ===========================================================================
# 10. SEGMENTATION HELPERS
# ===========================================================================

class Segments:
    """GAQL segment helpers for device, geo, network, hour, day of week.

    These return the SELECT fields and GROUP BY behavior to inject
    into a tool's GAQL query when segmentation is requested.

    IMPORTANT: Adding segments to GAQL splits metrics per segment.
    Only use when explicitly requested by agent/user.
    """

    # Available GAQL segments relevant to our tools
    AVAILABLE = {
        "device":       "segments.device",
        "network":      "segments.ad_network_type",
        "day_of_week":  "segments.day_of_week",
        "hour":         "segments.hour",
        "month":        "segments.month",
        "quarter":      "segments.quarter",
        "geo":          "segments.geo_target_region",
        "geo_city":     "segments.geo_target_city",
        "slot":         "segments.slot",                # top vs other
        "conversion_action": "segments.conversion_action_name",
    }

    @classmethod
    def get_segment_field(cls, segment_name: str) -> Optional[str]:
        """Return the GAQL segment field for a given name, or None."""
        return cls.AVAILABLE.get(segment_name.lower())

    @classmethod
    def list_available(cls) -> List[str]:
        """Return list of available segment names."""
        return list(cls.AVAILABLE.keys())

    @classmethod
    def inject_segment(
        cls,
        base_select: str,
        segment_name: str,
    ) -> str:
        """Add a segment field to an existing SELECT clause.

        Returns modified SELECT string, or original if segment unknown.
        """
        field = cls.get_segment_field(segment_name)
        if not field:
            logger.warning("Unknown segment: %s", segment_name)
            return base_select

        # Avoid duplicate
        if field in base_select:
            return base_select

        # Insert before FROM
        return base_select.replace(
            " FROM ", f", {field} FROM ", 1
        )


# ===========================================================================
# INTERNAL HELPERS
# ===========================================================================

def _parse_date(s: str) -> date:
    """Parse YYYY-MM-DD string to date object."""
    from datetime import datetime
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def _format_delta(current: float, previous: float) -> str:
    """Calculate and format percentage change."""
    if previous == 0:
        if current == 0:
            return "0.0%"
        return "+∞" if current > 0 else "-∞"
    delta = ((current - previous) / previous) * 100
    sign = "+" if delta > 0 else ""
    return f"{sign}{delta:.1f}%"


# ===========================================================================
# CONVENIENCE: Full pipeline function
# ===========================================================================

def process_rows(
    rows: List[Dict[str, Any]],
    text_field: str = "",
    contains: str = "",
    excludes: str = "",
    min_clicks: int = 0,
    min_impressions: int = 0,
    min_conversions: float = 0,
    max_cpa: float = 0,
    min_roas: float = 0,
    min_ctr: float = 0,
    max_cpc: float = 0,
    min_spend: float = 0,
    max_spend: float = 0,
    zero_conversions: bool = False,
    status: str = "",
    campaign_type: str = "",
    sort_by: str = "spend",
    ascending: bool = False,
    limit: int = 50,
) -> Tuple[List[Dict[str, Any]], int, bool, str]:
    """Full pipeline: filter → sort → limit in ONE call.

    Returns: (rows, total_count, was_truncated, filter_description)

    Usage:
        rows, total, truncated, filter_desc = process_rows(
            aggregated_data,
            text_field="term",
            contains="pacco,pacchi",
            excludes="poste italiane",
            min_clicks=5,
            sort_by="spend",
            limit=100,
        )
    """
    # Build filter description BEFORE filtering (for header)
    filter_desc = build_filter_description(
        contains=contains, excludes=excludes,
        min_clicks=min_clicks, min_impressions=min_impressions,
        min_conversions=min_conversions, max_cpa=max_cpa,
        min_roas=min_roas, min_ctr=min_ctr,
        max_cpc=max_cpc, min_spend=min_spend,
        max_spend=max_spend, zero_conversions=zero_conversions,
        status=status, campaign_type=campaign_type,
    )

    # Filter
    filtered = apply_filters(
        rows,
        text_field=text_field, contains=contains, excludes=excludes,
        min_clicks=min_clicks, min_impressions=min_impressions,
        min_conversions=min_conversions, max_cpa=max_cpa,
        min_roas=min_roas, min_ctr=min_ctr,
        max_cpc=max_cpc, min_spend=min_spend,
        max_spend=max_spend, zero_conversions=zero_conversions,
        status=status, campaign_type=campaign_type,
    )

    # Sort
    sorted_rows = apply_sort(filtered, sort_by=sort_by, ascending=ascending)

    # Limit
    limited, total, truncated = apply_limit(sorted_rows, limit=limit)

    return limited, total, truncated, filter_desc
