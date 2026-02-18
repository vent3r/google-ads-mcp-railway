"""R7: Auction insights (competitor analysis)."""

import logging
from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    ResultFormatter,
    run_query,
)
from tools.options import format_output, build_header

logger = logging.getLogger(__name__)


@mcp.tool()
def auction_insights(
    client: str,
    campaign: str,
    date_from: str,
    date_to: str,
) -> str:
    """Analyze auction competition for a campaign.

    Shows which competitors appear alongside your ads with impression share,
    overlap rate, position above rate, and outranking share.

    USE THIS TOOL WHEN:
    - User asks about competitors, auction data
    - "chi sono i competitor", "auction insights", "concorrenza"
    - Understanding competitive landscape

    DO NOT USE WHEN:
    - Performance metrics → use campaign_analysis
    - Search terms → use search_term_analysis

    OUTPUT: Table with competitor domains and auction metrics.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID (REQUIRED — auction insights requires campaign filter).
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    campaign_id = CampaignResolver.resolve(customer_id, campaign)

    q = (
        "SELECT auction_insight.display_domain, "
        "auction_insight.impression_share, "
        "auction_insight.overlap_rate, "
        "auction_insight.position_above_rate, "
        "auction_insight.top_of_page_rate, "
        "auction_insight.outranking_share "
        f"FROM auction_insight "
        f"WHERE campaign.id = {campaign_id} "
        f"AND {DateHelper.date_condition(date_from, date_to)}"
    )
    rows = run_query(customer_id, q)

    if not rows:
        return "No auction insight data found for this campaign and period."

    results = []
    for row in rows:
        is_val = float(row.get("auction_insight.impression_share", 0) or 0)
        overlap = float(row.get("auction_insight.overlap_rate", 0) or 0)
        pos_above = float(row.get("auction_insight.position_above_rate", 0) or 0)
        top_page = float(row.get("auction_insight.top_of_page_rate", 0) or 0)
        outranking = float(row.get("auction_insight.outranking_share", 0) or 0)

        results.append({
            "domain": row.get("auction_insight.display_domain", ""),
            "impression_share": f"{is_val * 100:.1f}%" if is_val <= 1 else f"{is_val:.1f}%",
            "overlap": f"{overlap * 100:.1f}%" if overlap <= 1 else f"{overlap:.1f}%",
            "position_above": f"{pos_above * 100:.1f}%" if pos_above <= 1 else f"{pos_above:.1f}%",
            "top_of_page": f"{top_page * 100:.1f}%" if top_page <= 1 else f"{top_page:.1f}%",
            "outranking": f"{outranking * 100:.1f}%" if outranking <= 1 else f"{outranking:.1f}%",
            "_sort": is_val,
        })

    results.sort(key=lambda r: r["_sort"], reverse=True)
    for r in results:
        r.pop("_sort")

    columns = [
        ("domain", "Competitor"),
        ("impression_share", "Imp Share"),
        ("overlap", "Overlap"),
        ("position_above", "Pos Above"),
        ("top_of_page", "Top of Page"),
        ("outranking", "Outranking"),
    ]

    header = build_header(
        title="Auction Insights",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        extra=f"{len(results)} competitors",
    )

    return format_output(results, columns, header=header, output_mode="full")
