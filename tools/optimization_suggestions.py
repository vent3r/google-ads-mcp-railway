"""R11: Proactive optimization suggestions."""
import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver

@mcp.tool()
def optimization_suggestions(client: str, campaign: str = "") -> str:
    """Get proactive optimization suggestions."""
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    
    return f"""# Optimization Suggestions for {client}

## Recommendations:
- Review budget allocation
- Check ad group performance
- Analyze quality scores
- Investigate high-CPA keywords
- Test new ad variations
"""
