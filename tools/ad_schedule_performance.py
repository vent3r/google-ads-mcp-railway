"""T12: Ad schedule performance — performance by day/time targeting rules."""

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
from tools.options import build_header, format_output

logger = logging.getLogger(__name__)


@mcp.tool()
def ad_schedule_performance(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
) -> str:
    """Analyze performance by ad schedule (day of week + time slot targeting).

    USE THIS TOOL WHEN:
    - User asks about ad schedule performance, day parting, time targeting
    - "performance per orario", "ad schedule", "day parting"
    - Investigating when ads perform best/worst

    DO NOT USE WHEN:
    - General hourly/daily breakdown -> use hour_day_analysis
    - Device breakdown -> use device_breakdown

    OUTPUT: Markdown table with day, time slot, bid adjustment, and metrics.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    date_cond = DateHelper.date_condition(date_from, date_to)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    q = (
        "SELECT "
        "campaign.name, "
        "campaign_criterion.ad_schedule.day_of_week, "
        "campaign_criterion.ad_schedule.start_hour, "
        "campaign_criterion.ad_schedule.end_hour, "
        "campaign_criterion.bid_modifier, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        f"FROM ad_schedule_view "
        f"WHERE {date_cond}{campaign_clause}"
    )
    rows = run_query(customer_id, q)

    # Aggregate by campaign + day + time slot
    by_schedule = defaultdict(lambda: {
        "campaign.name": "", "day": "", "time_slot": "", "bid_modifier": "",
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        camp_name = row.get("campaign.name", "")
        day = str(row.get("campaign_criterion.ad_schedule.day_of_week", "")).replace("_", " ").title()
        start_h = row.get("campaign_criterion.ad_schedule.start_hour", "")
        end_h = row.get("campaign_criterion.ad_schedule.end_hour", "")
        time_slot = f"{start_h}:00-{end_h}:00" if start_h != "" and end_h != "" else ""
        bid_mod = row.get("campaign_criterion.bid_modifier", "")

        key = (camp_name, day, time_slot)
        a = by_schedule[key]
        a["campaign.name"] = camp_name
        a["day"] = day
        a["time_slot"] = time_slot
        if bid_mod:
            mod_val = float(bid_mod) if bid_mod else 0
            a["bid_modifier"] = f"{mod_val:+.0%}" if mod_val != 0 else "0%"
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    # Day order for sorting
    day_order = {
        "Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3,
        "Friday": 4, "Saturday": 5, "Sunday": 6,
    }

    results = []
    for a in by_schedule.values():
        compute_derived_metrics(a)
        results.append(a)

    results.sort(key=lambda r: (r["campaign.name"], day_order.get(r["day"], 9), r["time_slot"]))

    columns = [
        ("campaign.name", "Campaign"),
        ("day", "Day"),
        ("time_slot", "Time Slot"),
        ("bid_modifier", "Bid Adj"),
        ("metrics.impressions", "Impr"),
        ("metrics.clicks", "Clicks"),
        ("_spend", "Spend \u20ac"),
        ("metrics.conversions", "Conv"),
        ("_cpa", "CPA \u20ac"),
    ]

    header = build_header(
        title="Ad Schedule Performance",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        extra=f"{len(results)} schedule entries",
    )

    return format_output(results, columns, header=header, output_mode="full")
