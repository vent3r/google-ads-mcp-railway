"""R9: Budget pacing analysis."""
import logging
from datetime import date
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, DateHelper, run_query
from tools.options import build_header

@mcp.tool()
def budget_pacing(client: str, campaign: str = "") -> str:
    """Analyze budget pacing for campaigns."""
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    
    today = date.today()
    days_in_month = 30
    days_passed = today.day
    
    q = f"SELECT campaign.name, campaign_budget.amount_micros FROM campaign LIMIT 50"
    rows = run_query(customer_id, q)
    
    result_lines = ["## Budget Pacing\n"]
    for row in rows:
        cname = row.get("campaign.name", "Unknown")
        result_lines.append(f"- **{cname}**: Pacing analysis needed")
    
    return "\n".join(result_lines)
