"""GA4 tools: ga4_list_properties, ga4_run_report, ga4_item_report, ga4_event_check.

Read-only access to Google Analytics 4 via the Data API (v1beta) and the
Admin API (account summaries). Auth: see tools/ga4_helpers.py.
"""

import logging
from typing import List

from ads_mcp.coordinator import mcp
from tools.ga4_helpers import (
    PropertyResolver,
    fmt_num,
    ga4_date_range,
    run_ga4_report,
)
from tools.helpers import ResultFormatter

logger = logging.getLogger(__name__)


def _csv(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


# ---------------------------------------------------------------------------
# 1. List properties
# ---------------------------------------------------------------------------

@mcp.tool()
def ga4_list_properties() -> str:
    """List all GA4 properties the server credentials can read.

    USE THIS TOOL FIRST when:
    - You need a GA4 property ID for another ga4_* tool
    - The user names a site/client and you don't know its property

    OUTPUT: Table with account, property name, property ID.
    """
    props = PropertyResolver.get_all()
    if not props:
        return "No GA4 properties visible. Add the service account as Viewer on the property."
    columns = [
        ("account", "Account"),
        ("name", "Property"),
        ("id", "Property ID"),
        ("account_id", "Account ID"),
    ]
    return f"**GA4 Properties** ({len(props)})\n\n" + ResultFormatter.markdown_table(
        props, columns, max_rows=200
    )


# ---------------------------------------------------------------------------
# 2. Generic report
# ---------------------------------------------------------------------------

@mcp.tool()
def ga4_run_report(
    property: str,
    dimensions: str,
    metrics: str,
    period: str = "last_7_days",
    filter_field: str = "",
    filter_value: str = "",
    order_by: str = "",
    limit: int = 50,
) -> str:
    """Run a custom GA4 report (Data API runReport). Read-only.

    USE THIS TOOL WHEN:
    - No dedicated ga4_* tool covers the request
    - You need specific dimensions/metrics (channel, landing page, device...)

    Dimension and metric names are GA4 API names (camelCase):
    sessionDefaultChannelGroup, landingPage, deviceCategory, eventName,
    itemId, itemName, date / sessions, totalUsers, conversions, purchaseRevenue,
    eventCount, itemsAddedToCart, itemsPurchased, itemRevenue.
    Item-scoped metrics (items*) need item-scoped dimensions (item*).

    Args:
        property: GA4 property name or numeric ID.
        dimensions: Comma-separated dimension names.
        metrics: Comma-separated metric names.
        period: last_7_days | last_30_days | yesterday | today | 'YYYY-MM-DD,YYYY-MM-DD'.
        filter_field: Optional dimension to filter on (exact match).
        filter_value: Value for filter_field.
        order_by: Metric to sort by desc (default: first metric).
        limit: Max rows to display (default 50).
    """
    pid = PropertyResolver.resolve(property)
    pname = PropertyResolver.resolve_name(pid)
    dims = _csv(dimensions)
    mets = _csv(metrics)
    if not dims or not mets:
        return "Error: provide at least one dimension and one metric."
    start, end = ga4_date_range(period)

    flt = {"field": filter_field, "value": filter_value} if filter_field and filter_value else None
    rows = run_ga4_report(
        pid, dims, mets, start, end,
        dimension_filter=flt,
        order_by_metric=order_by or mets[0],
        limit=max(limit, 1000),
    )
    if not rows:
        return f"**GA4 Report** · {pname} · {start} → {end}\n\nNo data."

    total = len(rows)
    display = [{k: fmt_num(v) for k, v in r.items()} for r in rows[:limit]]
    columns = [(d, d) for d in dims] + [(m, m) for m in mets]
    out = f"**GA4 Report** · {pname} ({pid}) · {start} → {end} · {total:,} rows\n\n"
    out += ResultFormatter.markdown_table(display, columns, max_rows=limit)
    return out


# ---------------------------------------------------------------------------
# 3. Item (product) report
# ---------------------------------------------------------------------------

@mcp.tool()
def ga4_item_report(
    property: str,
    period: str = "last_7_days",
    sort_by: str = "itemsAddedToCart",
    limit: int = 50,
    item_ids: str = "",
) -> str:
    """Product-level e-commerce report: add to cart, views, purchases, revenue by item.

    USE THIS TOOL WHEN:
    - User asks which products get the most add-to-carts / purchases
    - You need product demand data to select items for a campaign
    - You need to check that itemId in GA4 matches a product feed

    OUTPUT: Table with itemId, itemName, itemsViewed, itemsAddedToCart,
    itemsPurchased, itemRevenue — sorted by sort_by desc.

    Args:
        property: GA4 property name or numeric ID.
        period: last_7_days | last_30_days | 'YYYY-MM-DD,YYYY-MM-DD'.
        sort_by: itemsAddedToCart | itemsViewed | itemsPurchased | itemRevenue.
        limit: Max rows to display (default 50).
        item_ids: Optional comma-separated itemId list to restrict to.
    """
    pid = PropertyResolver.resolve(property)
    pname = PropertyResolver.resolve_name(pid)
    start, end = ga4_date_range(period)

    dims = ["itemId", "itemName"]
    mets = ["itemsViewed", "itemsAddedToCart", "itemsPurchased", "itemRevenue"]
    if sort_by not in mets:
        return f"Error: sort_by must be one of {', '.join(mets)}."

    flt = {"field": "itemId", "in": _csv(item_ids)} if item_ids else None
    rows = run_ga4_report(
        pid, dims, mets, start, end,
        dimension_filter=flt,
        order_by_metric=sort_by,
        limit=max(limit, 5000),
    )
    if not rows:
        return f"**GA4 Items** · {pname} · {start} → {end}\n\nNo item data."

    total = len(rows)
    sum_atc = sum(float(r.get("itemsAddedToCart", 0) or 0) for r in rows)
    sum_pur = sum(float(r.get("itemsPurchased", 0) or 0) for r in rows)
    sum_rev = sum(float(r.get("itemRevenue", 0) or 0) for r in rows)

    display = [{k: fmt_num(v) for k, v in r.items()} for r in rows[:limit]]
    columns = [
        ("itemId", "Item ID"),
        ("itemName", "Item name"),
        ("itemsViewed", "Viewed"),
        ("itemsAddedToCart", "Add to cart"),
        ("itemsPurchased", "Purchased"),
        ("itemRevenue", "Revenue"),
    ]
    out = (
        f"**GA4 Items** · {pname} ({pid}) · {start} → {end} · "
        f"{total:,} items · ATC {sum_atc:,.0f} · Purchased {sum_pur:,.0f} · "
        f"Revenue {sum_rev:,.2f}\n\n"
    )
    out += ResultFormatter.markdown_table(display, columns, max_rows=limit)
    return out


# ---------------------------------------------------------------------------
# 4. Event check
# ---------------------------------------------------------------------------

@mcp.tool()
def ga4_event_check(
    property: str,
    period: str = "last_7_days",
    events: str = "add_to_cart,begin_checkout,purchase,view_item",
) -> str:
    """Check that key events are firing in GA4, with daily counts.

    USE THIS TOOL WHEN:
    - Verifying tracking health before building automation on GA4 data
    - User asks "is add_to_cart tracked?", "how many purchases in GA4?"

    OUTPUT: Totals per event for the period, plus a per-day table.

    Args:
        property: GA4 property name or numeric ID.
        period: last_7_days | last_30_days | 'YYYY-MM-DD,YYYY-MM-DD'.
        events: Comma-separated event names to check.
    """
    pid = PropertyResolver.resolve(property)
    pname = PropertyResolver.resolve_name(pid)
    start, end = ga4_date_range(period)
    names = _csv(events)
    if not names:
        return "Error: provide at least one event name."

    rows = run_ga4_report(
        pid, ["date", "eventName"], ["eventCount", "totalUsers"], start, end,
        dimension_filter={"field": "eventName", "in": names},
        limit=10000,
    )

    totals = {n: 0 for n in names}
    by_day = {}
    for r in rows:
        ev = r.get("eventName", "")
        cnt = int(float(r.get("eventCount", 0) or 0))
        totals[ev] = totals.get(ev, 0) + cnt
        by_day.setdefault(r.get("date", ""), {})[ev] = cnt

    lines = [f"**GA4 Events** · {pname} ({pid}) · {start} → {end}\n"]
    for n in names:
        flag = "" if totals.get(n, 0) > 0 else "  ⚠ no events"
        lines.append(f"- `{n}`: {totals.get(n, 0):,}{flag}")

    if by_day:
        lines.append("")
        day_rows = []
        for d in sorted(by_day):
            row = {"date": f"{d[:4]}-{d[4:6]}-{d[6:]}" if len(d) == 8 else d}
            for n in names:
                row[n] = f"{by_day[d].get(n, 0):,}"
            day_rows.append(row)
        columns = [("date", "Date")] + [(n, n) for n in names]
        lines.append(ResultFormatter.markdown_table(day_rows, columns, max_rows=100))

    return "\n".join(lines)
