"""Tool 7: anomaly_detection — Detect anomalous days for a given metric.

Uses standard deviation analysis. Specialized logic — options.py used
for output formatting and header/footer only.
"""

import math
from collections import defaultdict
from datetime import date, timedelta

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    run_query,
)
from tools.options import (
    OutputFormat,
    build_header,
    format_output,
)


@mcp.tool()
def anomaly_detection(
    client: str,
    days: int = 30,
    metric: str = "spend",
    campaign: str = "",
    sensitivity: float = 2.0,
) -> str:
    """Detect anomalous days where a metric deviates from the mean.

    Uses standard deviation analysis to find spikes and drops.

    Args:
        client: Account name or customer ID.
        days: Number of days to analyze (default 30).
        metric: spend, clicks, conversions, cpa, cpc, or ctr (default spend).
        campaign: Campaign name or ID (optional).
        sensitivity: Std dev threshold — 1.5, 2.0, or 2.5 (default 2.0).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    date_from = DateHelper.format_date(start)
    date_to = DateHelper.format_date(end)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    query = (
        "SELECT segments.date, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        "FROM campaign "
        f"WHERE {DateHelper.date_condition(date_from, date_to)}"
        f"{campaign_clause}"
    )

    rows = run_query(customer_id, query)

    # Aggregate by day
    daily = defaultdict(lambda: {
        "impressions": 0, "clicks": 0, "cost_micros": 0,
        "conversions": 0.0, "conversions_value": 0.0,
    })

    for row in rows:
        d = row.get("segments.date", "")
        a = daily[d]
        a["impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    if not daily:
        return "No data found for the specified period."

    def get_value(a: dict) -> float:
        spend = a["cost_micros"] / 1_000_000
        clicks = a["clicks"]
        conv = a["conversions"]
        impr = a["impressions"]
        if metric == "spend":
            return spend
        elif metric == "clicks":
            return float(clicks)
        elif metric == "conversions":
            return conv
        elif metric == "cpa":
            return spend / conv if conv > 0 else 0.0
        elif metric == "cpc":
            return spend / clicks if clicks > 0 else 0.0
        elif metric == "ctr":
            return (clicks / impr * 100) if impr > 0 else 0.0
        return spend

    day_values = [
        {"date": d, "value": get_value(daily[d])}
        for d in sorted(daily.keys())
    ]

    values = [dv["value"] for dv in day_values]
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    std = math.sqrt(variance) if variance > 0 else 0.0

    anomalies = []
    if std > 0:
        for dv in day_values:
            dev = abs(dv["value"] - mean)
            if dev > sensitivity * std:
                anomalies.append({
                    "date": dv["date"],
                    "value": dv["value"],
                    "sigma": round(dev / std, 1),
                    "type": "spike" if dv["value"] > mean else "drop",
                })

    def fmt(v: float) -> str:
        if metric == "ctr":
            return f"{v:.1f}%"
        return f"{v:,.2f}"

    # Format output rows
    output = []
    for a in anomalies:
        output.append({
            "date": a["date"],
            "value": fmt(a["value"]),
            "mean": fmt(mean),
            "deviation": f"{a['sigma']}σ",
            "type": a["type"],
        })

    columns = [
        ("date", "Date"), ("value", "Value"), ("mean", "Mean"),
        ("deviation", "Deviation"), ("type", "Type"),
    ]

    header = build_header(
        title="Anomaly Detection",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        extra=f"Metric: {metric} · Sensitivity: {sensitivity}σ · {n} days · Mean: {fmt(mean)}/day · Std: {fmt(std)} · {len(anomalies)} anomalies",
    )

    if not anomalies:
        return header + "\n\nNo anomalies detected at current sensitivity."

    return format_output(output, columns, header=header)
