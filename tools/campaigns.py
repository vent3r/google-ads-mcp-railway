"""Tool 2: campaign_analysis — Campaign performance with period comparison."""

from ads_mcp.coordinator import mcp
from tools.helpers import (
    ClientResolver,
    DateHelper,
    ResultFormatter,
    compute_derived_metrics,
    run_query,
)


@mcp.tool()
def campaign_analysis(
    client: str,
    date_from: str,
    date_to: str,
    status_filter: str = "ENABLED",
) -> str:
    """Analyze campaign performance with automatic comparison to the previous period.

    Shows spend, clicks, conversions, CPA, ROAS and percentage changes
    versus the preceding period of equal length.

    Args:
        client: Account name or customer ID (e.g. "My Client" or "1234567890").
        date_from: Start date in YYYY-MM-DD format.
        date_to: End date in YYYY-MM-DD format.
        status_filter: Campaign status filter — ENABLED, PAUSED, or ALL (default ENABLED).
    """
    customer_id = ClientResolver.resolve(client)

    d_from = DateHelper.parse_date(date_from)
    d_to = DateHelper.parse_date(date_to)
    prev_from, prev_to = DateHelper.previous_period(d_from, d_to)

    status_clause = ""
    if status_filter.upper() != "ALL":
        status_clause = f" AND campaign.status = '{status_filter.upper()}'"

    fields = (
        "campaign.name, campaign.id, campaign.status, "
        "campaign.advertising_channel_type, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value, "
        "metrics.ctr, metrics.average_cpc"
    )

    # Current period
    query_current = (
        f"SELECT {fields} FROM campaign "
        f"WHERE {DateHelper.date_condition(date_from, date_to)}"
        f"{status_clause}"
    )
    current_rows = run_query(customer_id, query_current)

    # Previous period
    query_prev = (
        f"SELECT {fields} FROM campaign "
        f"WHERE {DateHelper.date_condition(DateHelper.format_date(prev_from), DateHelper.format_date(prev_to))}"
        f"{status_clause}"
    )
    prev_rows = run_query(customer_id, query_prev)

    # Index previous by campaign id
    prev_by_id = {}
    for row in prev_rows:
        compute_derived_metrics(row)
        cid = row.get("campaign.id")
        prev_by_id[cid] = row

    # Process current rows
    output = []
    for row in current_rows:
        compute_derived_metrics(row)
        cid = row.get("campaign.id")
        prev = prev_by_id.get(cid, {})

        prev_spend = prev.get("_spend", 0)
        prev_conv = float(prev.get("metrics.conversions", 0) or 0)

        output.append({
            "campaign": row.get("campaign.name", ""),
            "status": row.get("campaign.status", ""),
            "channel": row.get("campaign.advertising_channel_type", ""),
            "impressions": f"{int(row.get('metrics.impressions', 0) or 0):,}",
            "clicks": f"{int(row.get('metrics.clicks', 0) or 0):,}",
            "spend": ResultFormatter.format_currency(row["_spend"]),
            "conversions": f"{float(row.get('metrics.conversions', 0) or 0):,.1f}",
            "cpa": ResultFormatter.format_currency(row["_cpa"]),
            "roas": f"{row['_roas']:.2f}",
            "d_spend": ResultFormatter.format_delta(row["_spend"], prev_spend),
            "d_conv": ResultFormatter.format_delta(
                float(row.get("metrics.conversions", 0) or 0), prev_conv
            ),
        })

    # Sort by spend descending
    output.sort(key=lambda r: float(r["spend"].replace(",", "")), reverse=True)

    columns = [
        ("campaign", "Campaign"),
        ("status", "Status"),
        ("channel", "Channel"),
        ("impressions", "Impr"),
        ("clicks", "Clicks"),
        ("spend", "Spend"),
        ("conversions", "Conv"),
        ("cpa", "CPA"),
        ("roas", "ROAS"),
        ("d_spend", "Δ Spend%"),
        ("d_conv", "Δ Conv%"),
    ]

    header = (
        f"**Campaign Analysis** — {date_from} to {date_to}\n"
        f"Compared with previous period: {DateHelper.format_date(prev_from)} to {DateHelper.format_date(prev_to)}\n\n"
    )

    return header + ResultFormatter.markdown_table(output, columns, max_rows=50)
