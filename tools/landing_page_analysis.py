"""R8: Landing page performance."""
import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, DateHelper, compute_derived_metrics, run_query
from tools.options import format_output, build_header

@mcp.tool()
def landing_page_analysis(client: str, date_from: str, date_to: str, campaign: str = "", limit: int = 50) -> str:
    """Analyze landing page performance."""
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    
    q = f"SELECT landing_page_view.unexpanded_final_url, metrics.clicks, metrics.cost_micros, metrics.conversions FROM landing_page_view WHERE {DateHelper.date_condition(date_from, date_to)} LIMIT {limit}"
    rows = run_query(customer_id, q)
    for row in rows:
        compute_derived_metrics(row)
    
    cols = [("landing_page_view.unexpanded_final_url", "URL"), ("metrics.clicks", "Clicks"), ("_spend", "Spend €"), ("metrics.conversions", "Conv")]
    header = build_header("Landing Pages", client_name, date_from, date_to)
    return format_output(rows, cols, header=header)
