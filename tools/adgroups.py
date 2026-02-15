"""Tool 3: adgroup_analysis — Ad group performance with options.py pipeline.

GAQL → aggregate by ad_group.id → compute_derived_metrics →
process_rows (filter/sort/limit) → format_output.
"""

import logging

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    aggregate_rows,
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


@mcp.tool()
def adgroup_analysis(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    contains: str = "",
    excludes: str = "",
    status: str = "",
    min_clicks: int = 0,
    min_spend: float = 0,
    min_conversions: float = 0,
    max_cpa: float = 0,
    min_roas: float = 0,
    zero_conversions: bool = False,
    sort_by: str = "spend",
    limit: int = 50,
    output_mode: str = "summary",
) -> str:
    """Analyze ad group performance, optionally filtered by campaign.

    Aggregates per-day rows into one row per ad group, computes derived
    metrics, applies universal filters and sorts.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID to filter (optional).
        contains: Comma-separated — keep ad groups whose name contains ANY of these.
        excludes: Comma-separated — remove ad groups whose name contains ANY of these.
        status: Filter by status: ENABLED, PAUSED, or empty for all.
        min_clicks: Minimum clicks (default 0).
        min_spend: Minimum spend € (default 0).
        min_conversions: Minimum conversions (default 0).
        max_cpa: Maximum CPA € — 0 = no limit (default 0).
        min_roas: Minimum ROAS — 0 = no limit (default 0).
        zero_conversions: If true, only show ad groups with 0 conversions (default false).
        sort_by: spend, clicks, conversions, cpa, roas, ctr (default spend).
        limit: Max rows (default 50).
        output_mode: "summary" (top 10 + totals) or "full" (all rows). Default summary.
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    query = (
        "SELECT "
        "campaign.name, ad_group.name, ad_group.id, ad_group.status, "
        "ad_group.type, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        "FROM ad_group "
        f"WHERE {DateHelper.date_condition(date_from, date_to)}"
        f"{campaign_clause}"
    )

    rows = run_query(customer_id, query)

    # Aggregate by ad_group.id
    aggregated = aggregate_rows(
        rows,
        group_by=["ad_group.id"],
        collect_fields={"campaign.name": "campaigns"},
    )

    # Carry over non-metric fields and compute derived metrics
    # Build lookup for ad group metadata from raw rows
    ag_meta = {}
    for row in rows:
        ag_id = row.get("ad_group.id", "")
        if ag_id not in ag_meta:
            ag_meta[ag_id] = {
                "ad_group.name": row.get("ad_group.name", ""),
                "ad_group.status": row.get("ad_group.status", ""),
                "ad_group.type": row.get("ad_group.type", ""),
            }

    for row in aggregated:
        ag_id = row.get("ad_group.id", "")
        meta = ag_meta.get(ag_id, {})
        row["ad_group.name"] = meta.get("ad_group.name", "")
        row["ad_group.status"] = meta.get("ad_group.status", "")
        row["ad_group.type"] = meta.get("ad_group.type", "")
        compute_derived_metrics(row)

    # Apply options pipeline
    filtered, total, truncated, filter_desc, all_summary = process_rows(
        aggregated,
        text_field="ad_group.name",
        contains=contains,
        excludes=excludes,
        status=status,
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
    alerts = Benchmarks.summarize_flags(filtered, name_field="ad_group.name")

    # Columns
    columns = COLUMNS.ADGROUP

    # Build output
    header = build_header(
        title="Ad Group Analysis",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        filter_desc=filter_desc,
    )
    footer = build_footer(total, len(filtered), truncated, all_summary)

    result = format_output(filtered, columns, header=header, footer=footer,
                           output_mode=output_mode, pre_summary=all_summary,
                           total_filtered=total)

    if alerts:
        result += f"\n\n{alerts}"

    return result
