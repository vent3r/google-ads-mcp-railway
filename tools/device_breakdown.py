"""R3: Device performance breakdown (mobile, desktop, tablet)."""

import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, DateHelper, compute_derived_metrics, run_query
from tools.options import format_output, build_header, build_footer

logger = logging.getLogger(__name__)


@mcp.tool()
def device_breakdown(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
) -> str:
    """Analyze performance by device (mobile, desktop, tablet).

    USE THIS TOOL WHEN:
    - Find device-specific performance issues
    - "performance per device", "device breakdown"

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
        from tools.name_resolver import resolve_campaign
        _, campaign_id = resolve_campaign(client, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    q = (
        f"SELECT segments.device, metrics.impressions, metrics.clicks, "
        f"metrics.cost_micros, metrics.conversions, metrics.conversions_value "
        f"FROM campaign WHERE {DateHelper.date_condition(date_from, date_to)} {campaign_clause}"
    )
    rows = run_query(customer_id, q)

    by_device = {}
    for row in rows:
        device = row.get("segments.device", "UNKNOWN")
        if device not in by_device:
            by_device[device] = {
                "device": device,
                "metrics.impressions": 0,
                "metrics.clicks": 0,
                "metrics.cost_micros": 0,
                "metrics.conversions": 0.0,
                "metrics.conversions_value": 0.0,
            }
        d = by_device[device]
        d["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        d["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        d["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        d["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        d["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    results = []
    for d in by_device.values():
        compute_derived_metrics(d)
        results.append(d)

    results.sort(key=lambda r: r.get("_spend", 0), reverse=True)

    columns = [
        ("device", "Device"),
        ("metrics.impressions", "Impressions"),
        ("metrics.clicks", "Clicks"),
        ("_ctr", "CTR %"),
        ("_spend", "Spend €"),
        ("metrics.conversions", "Conv"),
        ("_cpa", "CPA €"),
    ]

    header = build_header(
        title="Device Performance",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
    )

    return format_output(results, columns, header=header, output_mode="full")
