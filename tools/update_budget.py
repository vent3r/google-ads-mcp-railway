"""W1: Update campaign daily budget."""

import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, run_query
from tools.validation import (
    validate_mode,
    validate_budget_amount,
    euros_to_micros,
    micros_to_euros,
)
from tools.error_handler import (
    handle_google_ads_error,
    handle_validation_error,
    format_error_for_llm,
)
from tools.mutation import (
    MutationPreview,
    MutationResult,
    format_preview_for_llm,
    format_result_for_llm,
)
from tools.audit import get_audit_logger
from tools.name_resolver import resolve_campaign
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf import field_mask_pb2

logger = logging.getLogger(__name__)


def _get_ads_client():
    """Get Google Ads API client from coordinator."""
    from ads_mcp.coordinator import get_google_ads_client

    return get_google_ads_client()


@mcp.tool()
def update_budget(
    client: str,
    campaign: str,
    new_budget_eur: float,
    mode: str = "preview",
) -> str:
    """Update the daily budget of a campaign.

    USE THIS TOOL WHEN:
    - User asks to change, increase, decrease a campaign budget
    - "aumenta il budget", "riduci il budget", "imposta budget a X"

    ALWAYS call with mode="preview" first. Only use mode="execute" after user confirms.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID.
        new_budget_eur: New daily budget in EUR (min 1.00, max 50000.00).
        mode: "preview" (show changes) or "execute" (apply changes). Default preview.
    """
    # Validation
    if not validate_mode(mode):
        return format_error_for_llm(
            handle_validation_error("mode must be 'preview' or 'execute'", "mode")
        )
    if not validate_budget_amount(new_budget_eur):
        return format_error_for_llm(
            handle_validation_error(
                f"Budget €{new_budget_eur} out of range (1.00–50000.00)",
                "new_budget_eur",
            )
        )

    # Resolve names
    try:
        customer_id, campaign_id = resolve_campaign(client, campaign)
        client_name = ClientResolver.resolve_name(customer_id)
    except ValueError as e:
        return format_error_for_llm(handle_validation_error(str(e)))

    # Read current state
    try:
        q = (
            f"SELECT campaign.id, campaign.name, campaign.status, "
            f"campaign_budget.id, campaign_budget.amount_micros "
            f"FROM campaign WHERE campaign.id = {campaign_id} LIMIT 1"
        )
        rows = run_query(customer_id, q)
        if not rows:
            return format_error_for_llm(
                handle_validation_error(f"Campaign {campaign_id} not found")
            )
        current = rows[0]
        budget_id = current.get("campaign_budget.id")
        old_micros = int(current.get("campaign_budget.amount_micros", 0) or 0)
        old_eur = micros_to_euros(old_micros)
        campaign_name = current.get("campaign.name", campaign)
    except GoogleAdsException as ex:
        return format_error_for_llm(handle_google_ads_error(ex))

    # Calculate diff + warnings
    new_micros = euros_to_micros(new_budget_eur)
    change_pct = ((new_budget_eur - old_eur) / old_eur * 100) if old_eur > 0 else 0

    warnings = []
    if abs(change_pct) > 50:
        warnings.append(f"Budget change: {change_pct:+.1f}% (exceeds 50%)")
    if old_eur > 0 and new_budget_eur > old_eur * 3:
        warnings.append(f"New budget is {new_budget_eur / old_eur:.1f}x current")

    preview = MutationPreview(
        tool_name="update_budget",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Update budget: {campaign_name}",
        changes=[{
            "field": "Daily Budget",
            "old": f"€{old_eur:,.2f}",
            "new": f"€{new_budget_eur:,.2f}",
        }],
        warnings=warnings,
        estimated_impact=f"Change: {change_pct:+.1f}%",
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    # Execute
    try:
        ads_client = _get_ads_client()
        svc = ads_client.get_service("CampaignBudgetService")
        op = ads_client.get_type("CampaignBudgetOperation")
        budget_resource = svc.campaign_budget_path(customer_id, budget_id)
        op.update.resource_name = budget_resource
        op.update.amount_micros = new_micros
        op.update_mask = field_mask_pb2.FieldMask(paths=["amount_micros"])
        response = svc.mutate_campaign_budgets(customer_id=customer_id, operations=[op])

        result = MutationResult(
            success=True,
            resource_name=response.results[0].resource_name,
            resource_id=str(budget_id),
            message=f"Budget updated: €{old_eur:,.2f} → €{new_budget_eur:,.2f} ({change_pct:+.1f}%)",
        )

        # Audit log
        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="update_budget",
                action="update_budget",
                parameters={"campaign": campaign, "new_budget_eur": new_budget_eur},
                old_values={"budget_eur": old_eur},
                new_values={"budget_eur": new_budget_eur},
                success=True,
            )

        return format_result_for_llm(result)

    except GoogleAdsException as ex:
        error = handle_google_ads_error(ex)
        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="update_budget",
                action="update_budget",
                parameters={"campaign": campaign, "new_budget_eur": new_budget_eur},
                old_values={"budget_eur": old_eur},
                new_values={"budget_eur": new_budget_eur},
                success=False,
                error_message=error.message,
                request_id=error.request_id,
            )
        return format_error_for_llm(error)
