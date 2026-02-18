"""R6: Hour and day of week performance analysis."""

import logging
from collections import defaultdict

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    ResultFormatter,
    compute_derived_metrics,
    run_query,
)
from tools.options import build_header

logger = logging.getLogger(__name__)

DAY_NAMES = {
    "MONDAY": "Mon", "TUESDAY": "Tue", "WEDNESDAY": "Wed",
    "THURSDAY": "Thu", "FRIDAY": "Fri", "SATURDAY": "Sat", "SUNDAY": "Sun",
}
DAY_ORDER = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]


@mcp.tool()
def hour_day_analysis(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
) -> str:
    """Analyze performance by hour of day and day of week.

    USE THIS TOOL WHEN:
    - User asks about best/worst times for ads
    - "a che ora spendiamo di più", "quale giorno funziona meglio"
    - Planning ad scheduling or bid adjustments by time

    DO NOT USE WHEN:
    - Device performance → use device_breakdown
    - Geographic performance → use geo_breakdown

    OUTPUT: Two markdown tables — hourly and daily breakdown.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    # Hourly query
    q_hour = (
        "SELECT segments.hour, metrics.impressions, metrics.clicks, "
        "metrics.cost_micros, metrics.conversions, metrics.conversions_value "
        f"FROM campaign WHERE {DateHelper.date_condition(date_from, date_to)}{campaign_clause}"
    )
    # Daily query
    q_day = (
        "SELECT segments.day_of_week, metrics.impressions, metrics.clicks, "
        "metrics.cost_micros, metrics.conversions, metrics.conversions_value "
        f"FROM campaign WHERE {DateHelper.date_condition(date_from, date_to)}{campaign_clause}"
    )

    hour_rows = run_query(customer_id, q_hour)
    day_rows = run_query(customer_id, q_day)

    # Aggregate by hour
    by_hour = defaultdict(lambda: {
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })
    for row in hour_rows:
        h = str(row.get("segments.hour", ""))
        a = by_hour[h]
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    hour_results = []
    for h in range(24):
        key = str(h)
        a = by_hour.get(key, {"metrics.impressions": 0, "metrics.clicks": 0,
                               "metrics.cost_micros": 0, "metrics.conversions": 0.0,
                               "metrics.conversions_value": 0.0})
        a["hour"] = f"{h:02d}:00"
        compute_derived_metrics(a)
        hour_results.append(a)

    # Aggregate by day
    by_day = defaultdict(lambda: {
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })
    for row in day_rows:
        d = str(row.get("segments.day_of_week", ""))
        a = by_day[d]
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    day_results = []
    for d in DAY_ORDER:
        a = by_day.get(d, {"metrics.impressions": 0, "metrics.clicks": 0,
                           "metrics.cost_micros": 0, "metrics.conversions": 0.0,
                           "metrics.conversions_value": 0.0})
        a["day"] = DAY_NAMES.get(d, d)
        compute_derived_metrics(a)
        day_results.append(a)

    # Format hourly table
    header = build_header(
        title="Hour & Day Analysis",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
    )

    h_cols = [("hour", "Hour"), ("metrics.clicks", "Clicks"), ("_spend", "Spend €"),
              ("_ctr", "CTR %"), ("metrics.conversions", "Conv"), ("_cpa", "CPA €")]
    h_lines = ["\n### Hourly Performance\n"]
    h_lines.append("| " + " | ".join(c[1] for c in h_cols) + " |")
    h_lines.append("| " + " | ".join("---" for _ in h_cols) + " |")
    for r in hour_results:
        cells = [str(r.get(k, "")) for k, _ in h_cols]
        h_lines.append("| " + " | ".join(cells) + " |")

    d_cols = [("day", "Day"), ("metrics.clicks", "Clicks"), ("_spend", "Spend €"),
              ("_ctr", "CTR %"), ("metrics.conversions", "Conv"), ("_cpa", "CPA €")]
    d_lines = ["\n### Day of Week Performance\n"]
    d_lines.append("| " + " | ".join(c[1] for c in d_cols) + " |")
    d_lines.append("| " + " | ".join("---" for _ in d_cols) + " |")
    for r in day_results:
        cells = [str(r.get(k, "")) for k, _ in d_cols]
        d_lines.append("| " + " | ".join(cells) + " |")

    return header + "\n".join(h_lines) + "\n" + "\n".join(d_lines)
