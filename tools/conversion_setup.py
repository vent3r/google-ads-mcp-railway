"""Tool 9: conversion_setup — Audit conversion tracking configuration.

Shows all conversion actions with status, attribution model, lookback windows.
"""

import logging

from ads_mcp.coordinator import mcp
from tools.helpers import (
    ClientResolver,
    run_query,
)
from tools.options import (
    COLUMNS,
    build_header,
    format_output,
)

logger = logging.getLogger(__name__)


@mcp.tool()
def conversion_setup(
    client: str,
    status_filter: str = "ALL",
) -> str:
    """Audit conversion tracking setup for an account.

    Shows every conversion action with its configuration: status, category,
    attribution model, lookback window, and whether it's included in the
    'Conversions' column.

    Args:
        client: Account name or customer ID.
        status_filter: ENABLED, HIDDEN, REMOVED, or ALL (default ALL).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    status_clause = ""
    if status_filter.upper() != "ALL":
        status_clause = (
            f" AND conversion_action.status = '{status_filter.upper()}'"
        )

    query = (
        "SELECT "
        "conversion_action.name, "
        "conversion_action.id, "
        "conversion_action.status, "
        "conversion_action.type, "
        "conversion_action.category, "
        "conversion_action.include_in_conversions_metric, "
        "conversion_action.attribution_model_settings.attribution_model, "
        "conversion_action.click_through_lookback_window_days, "
        "conversion_action.value_settings.default_value, "
        "conversion_action.value_settings.always_use_default_value, "
        "conversion_action.counting_type "
        "FROM conversion_action"
        f" WHERE conversion_action.status != 'REMOVED'{status_clause}"
    )

    rows = run_query(customer_id, query)

    if not rows:
        return "No conversion actions found."

    output = []
    included_count = 0
    for row in rows:
        included = row.get("conversion_action.include_in_conversions_metric")
        if included:
            included_count += 1

        attr_model = str(row.get(
            "conversion_action.attribution_model_settings.attribution_model", ""
        )).replace("_", " ").title()

        conv_type = str(row.get("conversion_action.type", "")).replace("_", " ").title()
        category = str(row.get("conversion_action.category", "")).replace("_", " ").title()
        counting = str(row.get("conversion_action.counting_type", "")).replace("_", " ").title()

        lookback = row.get("conversion_action.click_through_lookback_window_days", "")

        output.append({
            "name": row.get("conversion_action.name", ""),
            "status": row.get("conversion_action.status", ""),
            "category": category,
            "included": "✓" if included else "✗",
            "attribution_model": attr_model,
            "lookback_window_days": f"{lookback}d" if lookback else "-",
            "counting_type": counting,
            "include_in_conversions": "✓" if included else "✗",
        })

    # Sort: included first, then by name
    output.sort(key=lambda r: (r["included"] != "✓", r["name"].lower()))

    columns = COLUMNS.CONVERSION_SETUP

    header = build_header(
        title="Conversion Setup Audit",
        client_name=client_name,
        extra=f"{len(output)} actions ({included_count} in Conversions column)",
    )

    return format_output(output, columns, header=header)
