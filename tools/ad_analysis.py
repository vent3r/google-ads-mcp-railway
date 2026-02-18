"""R4: Ad performance analysis."""
import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, DateHelper, compute_derived_metrics, run_query
from tools.options import format_output, build_header

logger = logging.getLogger(__name__)

@mcp.tool()
def ad_analysis(client: str, date_from: str, date_to: str, campaign: str = "", limit: int = 50) -> str:
    """Analyze ad performance with ad strength metrics."""
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    
    filter_clause = DateHelper.date_condition(date_from, date_to)
    if campaign:
        from tools.name_resolver import resolve_campaign
        _, cid = resolve_campaign(client, campaign)
        filter_clause += f" AND campaign.id = {cid}"
    
    q = f"SELECT ad_group_ad.ad.id, ad_group_ad.ad_strength, metrics.clicks, metrics.cost_micros, metrics.conversions FROM ad_group_ad WHERE {filter_clause} LIMIT {limit}"
    rows = run_query(customer_id, q)
    
    for row in rows:
        compute_derived_metrics(row)
    
    cols = [("ad_group_ad.ad.id", "Ad ID"), ("ad_group_ad.ad_strength", "Strength"), ("metrics.clicks", "Clicks"), ("_spend", "Spend €")]
    header = build_header("Ad Analysis", client_name, date_from, date_to)
    return format_output(rows, cols, header=header)
