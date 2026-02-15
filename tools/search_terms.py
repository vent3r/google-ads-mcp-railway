"""Tool 5: search_term_analysis — Search term analysis with options.py pipeline.

Server-side text filtering during aggregation loop (contains/excludes applied
BEFORE aggregation for efficiency). Then options.py for numeric filters/sort/limit.
"""

import logging
from collections import defaultdict

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    compute_derived_metrics,
    run_query,
)
from tools.options import (
    Benchmarks,
    COLUMNS,
    OutputFormat,
    build_filter_description,
    build_footer,
    build_header,
    format_output,
    process_rows,
    text_match,
)

logger = logging.getLogger(__name__)


@mcp.tool()
def search_term_analysis(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    contains: str = "",
    excludes: str = "",
    min_clicks: int = 1,
    min_spend: float = 0,
    min_conversions: float = 0,
    max_cpa: float = 0,
    min_roas: float = 0,
    zero_conversions: bool = False,
    sort_by: str = "spend",
    limit: int = 50,
    detail: bool = False,
    output_mode: str = "summary",
) -> str:
    """Analyze search terms with server-side aggregation and text filtering.

    Default: one row per unique search term (campaigns/adgroups shown as count).
    Detail mode: one row per search term × campaign × ad group.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
        contains: Comma-separated words — keep only terms containing ANY of these (e.g. "pacco,pacchi"). Case-insensitive.
        excludes: Comma-separated words — remove terms containing ANY of these (e.g. "dhl,ups"). Case-insensitive.
        min_clicks: Min clicks to include (default 1).
        min_spend: Minimum spend € (default 0).
        min_conversions: Minimum conversions (default 0).
        max_cpa: Max CPA € — 0 = no limit (default 0).
        min_roas: Minimum ROAS — 0 = no limit (default 0).
        zero_conversions: If true, only show terms with 0 conversions (default false).
        sort_by: spend, clicks, impressions, cpa, ctr, roas (default spend).
        limit: Max rows (default 50).
        detail: If true, show per campaign/adgroup breakdown (default false).
        output_mode: "summary" (top 10 + totals) or "full" (all rows). Default summary.
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    conditions = [
        DateHelper.date_condition(date_from, date_to),
        "metrics.impressions > 0",
    ]
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        conditions.append(f"campaign.id = {campaign_id}")

    query = (
        "SELECT "
        "campaign.name, ad_group.name, "
        "search_term_view.search_term, search_term_view.status, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        f"FROM search_term_view WHERE {' AND '.join(conditions)}"
    )

    rows = run_query(customer_id, query)

    # Aggregate with server-side text filtering
    if detail:
        group_key = lambda row: (
            row.get("search_term_view.search_term", ""),
            row.get("campaign.name", ""),
            row.get("ad_group.name", ""),
        )
    else:
        group_key = lambda row: (row.get("search_term_view.search_term", ""),)

    agg = defaultdict(lambda: {
        "term": "", "status": "",
        "campaigns": set(), "adgroups": set(),
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        term = row.get("search_term_view.search_term", "")
        if not term:
            continue

        # Server-side text filter DURING aggregation (efficient)
        if (contains or excludes) and not text_match(term, contains, excludes):
            continue

        key = group_key(row)
        a = agg[key]
        a["term"] = term
        a["status"] = row.get("search_term_view.status", "")
        a["campaigns"].add(row.get("campaign.name", ""))
        a["adgroups"].add(row.get("ad_group.name", ""))
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    # Finalize sets, compute metrics
    aggregated = []
    for a in agg.values():
        camp_set = a.pop("campaigns")
        ag_set = a.pop("adgroups")
        a["campaign.name"] = (
            f"{len(camp_set)} campaigns" if len(camp_set) > 1
            else next(iter(camp_set), "")
        )
        a["ad_group.name"] = (
            f"{len(ag_set)} ad groups" if len(ag_set) > 1
            else next(iter(ag_set), "")
        )
        compute_derived_metrics(a)
        aggregated.append(a)

    total_unique = len(agg)

    # Apply options pipeline (text filter already applied, pass empty contains/excludes)
    filtered, total, truncated, filter_desc, all_summary = process_rows(
        aggregated,
        text_field="",  # text already filtered during aggregation
        contains="",
        excludes="",
        min_clicks=min_clicks,
        min_spend=min_spend,
        min_conversions=min_conversions,
        max_cpa=max_cpa,
        min_roas=min_roas,
        zero_conversions=zero_conversions,
        sort_by=sort_by,
        limit=limit,
    )

    # Manually add text filter info since we skipped it in process_rows
    filter_desc = build_filter_description(
        contains=contains, excludes=excludes,
        min_clicks=min_clicks, min_spend=min_spend,
        min_conversions=min_conversions, max_cpa=max_cpa,
        min_roas=min_roas, zero_conversions=zero_conversions,
    )

    # Columns
    columns = COLUMNS.SEARCH_TERM_DETAIL if detail else COLUMNS.SEARCH_TERM

    # Build output
    header = build_header(
        title="Search Term Analysis",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        filter_desc=filter_desc,
        extra=f"{total_unique:,} unique terms",
    )
    footer = build_footer(total, len(filtered), truncated, all_summary)

    return format_output(filtered, columns, header=header, footer=footer,
                         output_mode=output_mode, pre_summary=all_summary,
                         total_filtered=total)
