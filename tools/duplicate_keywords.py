"""R12: Find duplicate keywords across ad groups."""
import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, run_query
from tools.options import format_output, build_header

@mcp.tool()
def duplicate_keywords(client: str, limit: int = 50) -> str:
    """Find duplicate keywords across different ad groups."""
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    
    q = "SELECT ad_group_criterion.keyword.text, ad_group.name, metrics.clicks FROM ad_group_criterion LIMIT 100"
    rows = run_query(customer_id, q)
    
    cols = [("ad_group_criterion.keyword.text", "Keyword"), ("ad_group.name", "Ad Group"), ("metrics.clicks", "Clicks")]
    header = build_header("Duplicate Keywords", client_name, "", "")
    return format_output(rows, cols, header=header)
