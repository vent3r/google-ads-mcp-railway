"""R6: Hour and day of week performance."""
import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, DateHelper, compute_derived_metrics, run_query
from tools.options import format_output, build_header

@mcp.tool()
def hour_day_analysis(client: str, date_from: str, date_to: str, campaign: str = "") -> str:
    """Analyze performance by hour and day of week."""
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    
    q_hour = f"SELECT segments.hour, metrics.clicks, metrics.cost_micros FROM campaign WHERE {DateHelper.date_condition(date_from, date_to)}"
    q_day = f"SELECT segments.day_of_week, metrics.clicks, metrics.cost_micros FROM campaign WHERE {DateHelper.date_condition(date_from, date_to)}"
    
    hours = run_query(customer_id, q_hour)
    days = run_query(customer_id, q_day)
    
    for h in hours:
        compute_derived_metrics(h)
    for d in days:
        compute_derived_metrics(d)
    
    cols_h = [("segments.hour", "Hour"), ("metrics.clicks", "Clicks"), ("_spend", "Spend €")]
    cols_d = [("segments.day_of_week", "Day"), ("metrics.clicks", "Clicks"), ("_spend", "Spend €")]
    
    h_output = format_output(hours, cols_h, header=build_header("Hourly Performance", client_name, date_from, date_to))
    d_output = format_output(days, cols_d, header=build_header("Day Performance", client_name, date_from, date_to))
    
    return h_output + "\n\n" + d_output
