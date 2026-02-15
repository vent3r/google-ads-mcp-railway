"""Tool 2: campaign_analysis — Campaign performance with options.py pipeline.

Full pipeline: GAQL → aggregate by campaign.id → compute_derived_metrics →
process_rows (filter/sort/limit) → format_output with benchmarks & alerts.
"""

import logging
from collections import defaultdict

from ads_mcp.coordinator import mcp
from tools.helpers import (
    ClientResolver,
    DateHelper,
    ResultFormatter,
    compute_derived_metrics,
    run_query,
)
from tools.options import (
    Benchmarks,
    COLUMNS,
    OutputFormat,
    build_footer,
    build_header,
    format_output,
    process_rows,
)

logger = logging.getLogger(__name__)

# GAQL fields for campaign queries
_FIELDS = (
    "campaign.name, campaign.id, campaign.status, "
    "campaign.advertising_channel_type, "
    "campaign.bidding_strategy_type, "
    "metrics.impressions, metrics.clicks, metrics.cost_micros, "
    "metrics.conversions, metrics.conversions_value, "
    "metrics.search_impression_share, "
    "metrics.search_budget_lost_impression_share, "
    "metrics.search_rank_lost_impression_share"
)


def _fetch_and_aggregate(customer_id: str, date_from: str, date_to: str,
                         status_clause: str) -> list:
    """Fetch campaign data, aggregate across days, compute derived metrics."""
    q = (
        f"SELECT {_FIELDS} FROM campaign "
        f"WHERE {DateHelper.date_condition(date_from, date_to)}{status_clause}"
    )
    rows = run_query(customer_id, q)

    by_id = {}
    for row in rows:
        cid = row.get("campaign.id")
        if cid not in by_id:
            by_id[cid] = {
                "campaign.name": row.get("campaign.name", ""),
                "campaign.id": cid,
                "campaign.status": row.get("campaign.status", ""),
                "campaign.advertising_channel_type": row.get(
                    "campaign.advertising_channel_type", ""
                ),
                "campaign.bidding_strategy_type": row.get(
                    "campaign.bidding_strategy_type", ""
                ),
                "metrics.impressions": 0,
                "metrics.clicks": 0,
                "metrics.cost_micros": 0,
                "metrics.conversions": 0.0,
                "metrics.conversions_value": 0.0,
                "_is_values": [],
                "_is_budget_values": [],
                "_is_rank_values": [],
            }
        a = by_id[cid]
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(
            row.get("metrics.conversions_value", 0) or 0
        )
        # Impression share: collect per-day values for averaging
        is_val = row.get("metrics.search_impression_share")
        if is_val and is_val not in (0, "0"):
            a["_is_values"].append(float(is_val))
        is_budget = row.get("metrics.search_budget_lost_impression_share")
        if is_budget and is_budget not in (0, "0"):
            a["_is_budget_values"].append(float(is_budget))
        is_rank = row.get("metrics.search_rank_lost_impression_share")
        if is_rank and is_rank not in (0, "0"):
            a["_is_rank_values"].append(float(is_rank))

    # Finalize: average IS, compute derived metrics
    result = []
    for a in by_id.values():
        vals = a.pop("_is_values")
        a["search_is"] = round(sum(vals) / len(vals) * 100, 1) if vals else 0.0
        vals = a.pop("_is_budget_values")
        a["budget_lost_is"] = round(sum(vals) / len(vals) * 100, 1) if vals else 0.0
        vals = a.pop("_is_rank_values")
        a["rank_lost_is"] = round(sum(vals) / len(vals) * 100, 1) if vals else 0.0
        compute_derived_metrics(a)
        result.append(a)

    return result


@mcp.tool()
def campaign_analysis(
    client: str,
    date_from: str,
    date_to: str,
    status_filter: str = "ENABLED",
    contains: str = "",
    excludes: str = "",
    campaign_type: str = "",
    min_clicks: int = 0,
    min_spend: float = 0,
    min_conversions: float = 0,
    max_cpa: float = 0,
    min_roas: float = 0,
    zero_conversions: bool = False,
    sort_by: str = "spend",
    limit: int = 50,
) -> str:
    """Analyze campaign performance with comparison to previous period.

    Shows spend, clicks, conversions, CPA, ROAS, impression share, bidding
    strategy, and delta vs previous period. Includes proactive benchmark alerts.

    Args:
        client: Account name or customer ID (e.g. "Spedire.com" or "1234567890").
        date_from: Start date in YYYY-MM-DD format.
        date_to: End date in YYYY-MM-DD format.
        status_filter: ENABLED, PAUSED, or ALL (default ENABLED).
        contains: Comma-separated — keep campaigns whose name contains ANY of these words.
        excludes: Comma-separated — remove campaigns whose name contains ANY of these words.
        campaign_type: Filter by channel type: SEARCH, SHOPPING, DISPLAY, PERFORMANCE_MAX, or empty for all.
        min_clicks: Minimum clicks (default 0).
        min_spend: Minimum spend in € (default 0).
        min_conversions: Minimum conversions (default 0).
        max_cpa: Maximum CPA in € — 0 = no limit (default 0).
        min_roas: Minimum ROAS — 0 = no limit (default 0).
        zero_conversions: If true, only show campaigns with 0 conversions (default false).
        sort_by: spend, clicks, conversions, cpa, roas, ctr, search_is, budget_lost (default spend).
        limit: Max rows (default 50).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    d_from = DateHelper.parse_date(date_from)
    d_to = DateHelper.parse_date(date_to)
    prev_from, prev_to = DateHelper.previous_period(d_from, d_to)

    status_clause = ""
    if status_filter.upper() != "ALL":
        status_clause = f" AND campaign.status = '{status_filter.upper()}'"

    # Fetch current and previous period
    current_rows = _fetch_and_aggregate(customer_id, date_from, date_to, status_clause)
    prev_rows = _fetch_and_aggregate(
        customer_id,
        DateHelper.format_date(prev_from),
        DateHelper.format_date(prev_to),
        status_clause,
    )

    # Build prev lookup by campaign.id
    prev_by_id = {r["campaign.id"]: r for r in prev_rows}

    # Add delta columns to current rows
    for row in current_rows:
        p = prev_by_id.get(row["campaign.id"], {})
        p_spend = p.get("_spend", 0)
        p_conv = float(p.get("metrics.conversions", 0) or 0)
        row["d_spend"] = ResultFormatter.fmt_delta(row["_spend"], p_spend) if p_spend else "-"
        row["d_conv"] = ResultFormatter.fmt_delta(
            float(row.get("metrics.conversions", 0)), p_conv
        ) if p_conv else "-"

    # Apply options pipeline: filter → sort → limit
    filtered, total, truncated, filter_desc = process_rows(
        current_rows,
        text_field="campaign.name",
        contains=contains,
        excludes=excludes,
        campaign_type=campaign_type,
        min_clicks=min_clicks,
        min_spend=min_spend,
        min_conversions=min_conversions,
        max_cpa=max_cpa,
        min_roas=min_roas,
        zero_conversions=zero_conversions,
        sort_by=sort_by,
        limit=limit,
    )

    # Benchmarks
    alerts = Benchmarks.summarize_flags(filtered, name_field="campaign.name")

    # Summary row
    summary = OutputFormat.summary_row(filtered) if filtered else None

    # Columns
    columns = COLUMNS.CAMPAIGN + [
        ("d_spend", "Δ Spend"),
        ("d_conv", "Δ Conv"),
    ]

    # Build output
    header = build_header(
        title="Campaign Analysis",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        filter_desc=filter_desc,
        extra=f"vs {DateHelper.format_date(prev_from)} → {DateHelper.format_date(prev_to)}",
    )
    footer = build_footer(total, len(filtered), truncated, summary)

    result = format_output(filtered, columns, header=header, footer=footer)

    if alerts:
        result += f"\n\n{alerts}"

    return result
