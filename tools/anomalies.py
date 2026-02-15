"""Tool 7: anomaly_detection — Detect anomalous days for a given metric."""

import math
from datetime import date, timedelta

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    ResultFormatter,
    run_query,
)


@mcp.tool()
def anomaly_detection(
    client: str,
    days: int = 30,
    metric: str = "spend",
    campaign: str = "",
    sensitivity: float = 2.0,
) -> str:
    """Detect anomalous days where a metric deviates significantly from the mean.

    Uses standard deviation analysis to identify spikes and drops in daily
    performance data.

    Args:
        client: Account name or customer ID.
        days: Number of days to analyze (default 30).
        metric: Metric to analyze — spend, clicks, conversions, cpa, cpc, or ctr (default spend).
        campaign: Campaign name or ID to filter (optional).
        sensitivity: Number of standard deviations for anomaly threshold — 1.5, 2.0, or 2.5 (default 2.0).
    """
    customer_id = ClientResolver.resolve(client)

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    date_from = DateHelper.format_date(start)
    date_to = DateHelper.format_date(end)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    query = (
        "SELECT "
        "segments.date, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value, "
        "metrics.ctr, metrics.average_cpc "
        "FROM campaign "
        f"WHERE {DateHelper.date_condition(date_from, date_to)}"
        f"{campaign_clause}"
    )

    rows = run_query(customer_id, query)

    # Aggregate by day
    daily: dict[str, dict] = {}
    for row in rows:
        d = row.get("segments.date", "")
        if d not in daily:
            daily[d] = {
                "impressions": 0,
                "clicks": 0,
                "cost_micros": 0,
                "conversions": 0.0,
                "conversions_value": 0.0,
                "ctr_sum": 0.0,
                "cpc_sum": 0.0,
                "count": 0,
            }
        agg = daily[d]
        agg["impressions"] += int(row.get("metrics.impressions", 0) or 0)
        agg["clicks"] += int(row.get("metrics.clicks", 0) or 0)
        agg["cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        agg["conversions"] += float(row.get("metrics.conversions", 0) or 0)
        agg["conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)
        agg["ctr_sum"] += float(row.get("metrics.ctr", 0) or 0)
        agg["cpc_sum"] += float(row.get("metrics.average_cpc", 0) or 0)
        agg["count"] += 1

    if not daily:
        return "No data found for the specified period."

    # Compute metric values per day
    def get_metric_value(agg: dict) -> float:
        spend = agg["cost_micros"] / 1_000_000
        clicks = agg["clicks"]
        conversions = agg["conversions"]
        if metric == "spend":
            return spend
        elif metric == "clicks":
            return float(clicks)
        elif metric == "conversions":
            return conversions
        elif metric == "cpa":
            return spend / conversions if conversions > 0 else 0.0
        elif metric == "cpc":
            return spend / clicks if clicks > 0 else 0.0
        elif metric == "ctr":
            impressions = agg["impressions"]
            return (clicks / impressions * 100) if impressions > 0 else 0.0
        return spend

    day_values = []
    for d in sorted(daily.keys()):
        val = get_metric_value(daily[d])
        day_values.append({"date": d, "value": val})

    # Statistics
    values = [dv["value"] for dv in day_values]
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    std_dev = math.sqrt(variance) if variance > 0 else 0.0

    # Find anomalies
    anomalies = []
    for dv in day_values:
        if std_dev > 0:
            deviation = abs(dv["value"] - mean)
            if deviation > sensitivity * std_dev:
                anomaly_type = "spike" if dv["value"] > mean else "drop"
                anomalies.append({
                    "date": dv["date"],
                    "value": dv["value"],
                    "mean": mean,
                    "deviation_sigma": round(deviation / std_dev, 1),
                    "type": anomaly_type,
                })

    # Format values for display
    def fmt(v: float) -> str:
        if metric in ("ctr",):
            return ResultFormatter.format_percent(v)
        return ResultFormatter.format_currency(v)

    output = []
    for a in anomalies:
        output.append({
            "date": a["date"],
            "value": fmt(a["value"]),
            "mean": fmt(a["mean"]),
            "deviation": f"{a['deviation_sigma']}σ",
            "type": a["type"],
        })

    columns = [
        ("date", "Date"),
        ("value", "Value"),
        ("mean", "Mean"),
        ("deviation", "Deviation"),
        ("type", "Type"),
    ]

    header = (
        f"**Anomaly Detection** — {date_from} to {date_to}\n"
        f"Metric: {metric} | Sensitivity: {sensitivity}σ\n"
        f"Analyzed {n} days. Mean {metric}: {fmt(mean)}/day. "
        f"Std dev: {fmt(std_dev)}. Found {len(anomalies)} anomalies.\n\n"
    )

    if not anomalies:
        return header + "No anomalies detected at the current sensitivity level."

    return header + ResultFormatter.markdown_table(output, columns, max_rows=50)
