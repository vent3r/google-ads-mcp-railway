"""R10: Campaign snapshot."""
import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, DateHelper, run_query
from tools.options import build_header

@mcp.tool()
def campaign_overview(client: str, campaign: str, date_from: str, date_to: str) -> str:
    """Get complete campaign snapshot."""
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    
    from tools.name_resolver import resolve_campaign
    _, cid = resolve_campaign(client, campaign)
    
    q = f"SELECT campaign.name, campaign.status, metrics.clicks, metrics.conversions, metrics.cost_micros FROM campaign WHERE campaign.id = {cid} AND {DateHelper.date_condition(date_from, date_to)} LIMIT 1"
    rows = run_query(customer_id, q)
    
    if rows:
        r = rows[0]
        return f"# Campaign: {r.get('campaign.name')}\n\n**Status**: {r.get('campaign.status')}\n\n- Clicks: {r.get('metrics.clicks')}\n- Conversions: {r.get('metrics.conversions')}"
    return "Campaign not found"
