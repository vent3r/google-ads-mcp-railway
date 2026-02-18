"""R13: Quality score breakdown analysis."""
import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, DateHelper, run_query
from tools.options import format_output, build_header

@mcp.tool()
def qs_breakdown(client: str, campaign: str = "", date_from: str = "", date_to: str = "", limit: int = 50) -> str:
    """Analyze quality score distribution and low-QS keywords."""
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    
    q = "SELECT ad_group_criterion.keyword.text, ad_group_criterion.quality_info.quality_score FROM ad_group_criterion WHERE ad_group_criterion.negative = FALSE LIMIT 50"
    rows = run_query(customer_id, q)
    
    cols = [("ad_group_criterion.keyword.text", "Keyword"), ("ad_group_criterion.quality_info.quality_score", "QS")]
    header = build_header("Quality Scores", client_name, "", "")
    return format_output(rows, cols, header=header)
