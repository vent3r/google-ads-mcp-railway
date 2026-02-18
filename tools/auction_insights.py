"""R7: Auction insights (competitor analysis)."""
import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, DateHelper, run_query
from tools.options import format_output, build_header

@mcp.tool()
def auction_insights(client: str, campaign: str, date_from: str, date_to: str) -> str:
    """Analyze auction competition for a campaign."""
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    
    from tools.name_resolver import resolve_campaign
    _, cid = resolve_campaign(client, campaign)
    
    q = f"SELECT auction_insight.competitor_id, auction_insight.impression_share, auction_insight.overlap_rate FROM auction_insight WHERE campaign.id = {cid} AND {DateHelper.date_condition(date_from, date_to)}"
    rows = run_query(customer_id, q)
    
    cols = [("auction_insight.competitor_id", "Competitor"), ("auction_insight.impression_share", "Imp Share %"), ("auction_insight.overlap_rate", "Overlap %")]
    header = build_header("Auction Insights", client_name, date_from, date_to)
    return format_output(rows, cols, header=header)
