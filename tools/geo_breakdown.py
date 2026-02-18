"""R5: Geographic performance breakdown."""
import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, DateHelper, compute_derived_metrics, run_query
from tools.options import format_output, build_header

@mcp.tool()
def geo_breakdown(client: str, date_from: str, date_to: str, campaign: str = "", limit: int = 50) -> str:
    """Analyze performance by country."""
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    
    q = f"SELECT geographic_view.country_criterion_id, metrics.clicks, metrics.cost_micros, metrics.conversions FROM geographic_view WHERE {DateHelper.date_condition(date_from, date_to)} LIMIT {limit}"
    rows = run_query(customer_id, q)
    for row in rows:
        compute_derived_metrics(row)
    
    cols = [("geographic_view.country_criterion_id", "Country"), ("metrics.clicks", "Clicks"), ("_spend", "Spend €"), ("metrics.conversions", "Conv")]
    header = build_header("Geographic Breakdown", client_name, date_from, date_to)
    return format_output(rows, cols, header=header)
