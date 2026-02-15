"""Tool 2: campaign_analysis — Campaign performance with period comparison.

Enhancements vs original:
- bidding_strategy_type, campaign_budget, advertising_channel_type
- search_impression_share, search_budget_lost_IS, search_rank_lost_IS
- proper aggregation (date range → 1 row per campaign)
"""

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
    """Analyze campaign performance with comparison to previous period.

    Shows spend, clicks, conversions, CPA, ROAS, bidding strategy, impression share
    and percentage changes versus the preceding period of equal length.

    Args:
        client: Account name or customer ID (e.g. "Spedire.com" or "1234567890").
        date_from: Start date in YYYY-MM-DD format.
        date_to: End date in YYYY-MM-DD format.
        status_filter: ENABLED, PAUSED, or ALL (default ENABLED).
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
        "campaign.bidding_strategy_type, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value, "
        "metrics.search_impression_share, "
        "metrics.search_budget_lost_impression_share, "
        "metrics.search_rank_lost_impression_share"
    )

    def fetch_and_aggregate(df: str, dt: str) -> dict:
        """Fetch campaign data and aggregate across days into {campaign_id: row}."""
        q = (
            f"SELECT {fields} FROM campaign "
            f"WHERE {DateHelper.date_condition(df, dt)}{status_clause}"
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
            # Impression share: collect per-day values, then average
            is_val = row.get("metrics.search_impression_share")
            if is_val and is_val not in (0, "0"):
                a["_is_values"].append(float(is_val))
            is_budget = row.get("metrics.search_budget_lost_impression_share")
            if is_budget and is_budget not in (0, "0"):
                a["_is_budget_values"].append(float(is_budget))
            is_rank = row.get("metrics.search_rank_lost_impression_share")
            if is_rank and is_rank not in (0, "0"):
                a["_is_rank_values"].append(float(is_rank))

        # Finalize impression share averages
        for a in by_id.values():
            vals = a.pop("_is_values")
            a["_is"] = round(sum(vals) / len(vals) * 100, 1) if vals else None
            vals = a.pop("_is_budget_values")
            a["_is_budget"] = round(sum(vals) / len(vals) * 100, 1) if vals else None
            vals = a.pop("_is_rank_values")
            a["_is_rank"] = round(sum(vals) / len(vals) * 100, 1) if vals else None
            compute_derived_metrics(a)

        return by_id

    current = fetch_and_aggregate(date_from, date_to)
    prev = fetch_and_aggregate(
        DateHelper.format_date(prev_from), DateHelper.format_date(prev_to)
    )

    # Build output
    output = []
    for cid, row in current.items():
        p = prev.get(cid, {})
        p_spend = p.get("_spend", 0)
        p_conv = float(p.get("metrics.conversions", 0) or 0)

        # Clean bidding strategy name
        bidding = str(row.get("campaign.bidding_strategy_type", ""))
        bidding = bidding.replace("_", " ").title()

        # Clean channel type
        channel = str(row.get("campaign.advertising_channel_type", ""))
        channel = channel.replace("_", " ").title()

        output.append({
            "campaign": row.get("campaign.name", ""),
            "channel": channel,
            "bidding": bidding,
            "spend": ResultFormatter.fmt_currency(row["_spend"]),
            "conv": f"{float(row.get('metrics.conversions', 0) or 0):,.1f}",
            "cpa": ResultFormatter.fmt_currency(row["_cpa"]),
            "roas": f"{row['_roas']:.2f}",
            "is": ResultFormatter.fmt_percent(row["_is"]) if row["_is"] is not None else "-",
            "is_lost_budget": (
                ResultFormatter.fmt_percent(row["_is_budget"])
                if row["_is_budget"] is not None else "-"
            ),
            "d_spend": ResultFormatter.fmt_delta(row["_spend"], p_spend),
            "d_conv": ResultFormatter.fmt_delta(
                float(row.get("metrics.conversions", 0) or 0), p_conv
            ),
        })

    output.sort(key=lambda r: float(r["spend"].replace(",", "")), reverse=True)

    columns = [
        ("campaign", "Campaign"),
        ("channel", "Channel"),
        ("bidding", "Bidding"),
        ("spend", "Spend"),
        ("conv", "Conv"),
        ("cpa", "CPA"),
        ("roas", "ROAS"),
        ("is", "IS%"),
        ("is_lost_budget", "Lost Budget%"),
        ("d_spend", "Δ Spend"),
        ("d_conv", "Δ Conv"),
    ]

    header = (
        f"**Campaign Analysis** — {date_from} to {date_to}\n"
        f"vs previous: {DateHelper.format_date(prev_from)} to "
        f"{DateHelper.format_date(prev_to)}\n\n"
    )

    return header + ResultFormatter.markdown_table(output, columns, max_rows=50)
