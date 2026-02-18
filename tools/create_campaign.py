"""W8: Create a new campaign."""

import logging
import ads_mcp.utils as utils
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver
from tools.validation import validate_mode, validate_budget_amount, validate_enum, euros_to_micros
from tools.error_handler import handle_google_ads_error, handle_validation_error, format_error_for_llm
from tools.mutation import MutationPreview, MutationResult, format_preview_for_llm, format_result_for_llm
from tools.audit import get_audit_logger
from google.ads.googleads.errors import GoogleAdsException

logger = logging.getLogger(__name__)


@mcp.tool()
def create_campaign(
    client: str,
    name: str,
    budget_eur: float,
    campaign_type: str = "SEARCH",
    bidding_strategy: str = "MANUAL_CPC",
    target_roas: float = 0.0,
    target_cpa_eur: float = 0.0,
    mode: str = "preview",
) -> str:
    """Create a new campaign.

    USE THIS TOOL WHEN:
    - User wants to create a new campaign
    - "crea una nuova campagna", "nuovo campaign"

    ALWAYS call with mode="preview" first.

    Args:
        client: Account name or customer ID.
        name: Campaign name (unique).
        budget_eur: Daily budget EUR (1.00-50000.00).
        campaign_type: SEARCH, SHOPPING, DISPLAY, PERFORMANCE_MAX (default SEARCH).
        bidding_strategy: MANUAL_CPC, MAXIMIZE_CONVERSIONS, TARGET_ROAS, TARGET_CPA (default MANUAL_CPC).
        target_roas: ROAS target (required if bidding_strategy=TARGET_ROAS).
        target_cpa_eur: CPA target EUR (required if bidding_strategy=TARGET_CPA).
        mode: "preview" or "execute". Default preview.
    """
    if not validate_mode(mode):
        return format_error_for_llm(handle_validation_error("mode must be 'preview' or 'execute'", "mode"))
    if not validate_budget_amount(budget_eur):
        return format_error_for_llm(handle_validation_error(f"Budget out of range", "budget_eur"))
    if not validate_enum(campaign_type, ["SEARCH", "SHOPPING", "DISPLAY", "PERFORMANCE_MAX"]):
        return format_error_for_llm(handle_validation_error("Invalid campaign_type", "campaign_type"))
    if not validate_enum(bidding_strategy, ["MANUAL_CPC", "MAXIMIZE_CONVERSIONS", "TARGET_ROAS", "TARGET_CPA"]):
        return format_error_for_llm(handle_validation_error("Invalid bidding_strategy", "bidding_strategy"))

    try:
        customer_id = ClientResolver.resolve(client)
        client_name = ClientResolver.resolve_name(customer_id)
    except ValueError as e:
        return format_error_for_llm(handle_validation_error(str(e)))

    warnings = ["Campaign will be created as PAUSED"]

    preview = MutationPreview(
        tool_name="create_campaign",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Create campaign: {name}",
        changes=[
            {"field": "Name", "old": "—", "new": name},
            {"field": "Daily Budget", "old": "—", "new": f"€{budget_eur:,.2f}"},
            {"field": "Type", "old": "—", "new": campaign_type},
            {"field": "Bidding Strategy", "old": "—", "new": bidding_strategy},
            {"field": "Status", "old": "—", "new": "PAUSED"},
        ],
        warnings=warnings,
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    try:
        # Step 1: Create budget
        budget_svc = utils.get_googleads_service("CampaignBudgetService")
        budget_op = utils.get_googleads_type("CampaignBudgetOperation")
        budget_op.create.name = f"{name} Budget"
        budget_op.create.amount_micros = euros_to_micros(budget_eur)
        budget_response = budget_svc.mutate_campaign_budgets(customer_id=customer_id, operations=[budget_op])
        budget_id = budget_response.results[0].resource_name.split("/")[-1]

        # Step 2: Create campaign
        campaign_svc = utils.get_googleads_service("CampaignService")
        campaign_op = utils.get_googleads_type("CampaignOperation")
        campaign = campaign_op.create
        campaign.name = name
        campaign.campaign_budget = campaign_svc.campaign_budget_path(customer_id, budget_id)
        campaign.status = utils._googleads_client.enums.CampaignStatusEnum.CampaignStatus.PAUSED
        campaign.advertising_channel_type = (
            utils._googleads_client.enums.AdvertisingChannelTypeEnum.AdvertisingChannelType[campaign_type]
        )

        # Bidding strategy
        if bidding_strategy == "MANUAL_CPC":
            campaign.manual_cpc.enhanced_cpc_enabled = False
        elif bidding_strategy == "MAXIMIZE_CONVERSIONS":
            campaign.maximize_conversions.SetInParent()
        elif bidding_strategy == "TARGET_ROAS" and target_roas > 0:
            campaign.target_roas.target_roas = target_roas
        elif bidding_strategy == "TARGET_CPA" and target_cpa_eur > 0:
            campaign.target_cpa.target_cpa_micros = int(target_cpa_eur * 1_000_000)

        campaign_response = campaign_svc.mutate_campaigns(customer_id=customer_id, operations=[campaign_op])

        result = MutationResult(
            success=True,
            resource_id=campaign_response.results[0].resource_name.split("/")[-1],
            message=f"Campaign '{name}' created (PAUSED). Budget: €{budget_eur:,.2f}. Type: {campaign_type}.",
        )

        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="create_campaign",
                action="create",
                parameters={"name": name, "budget_eur": budget_eur, "campaign_type": campaign_type, "bidding_strategy": bidding_strategy},
                old_values={},
                new_values={"campaign_id": result.resource_id},
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
                tool_name="create_campaign",
                action="create",
                parameters={"name": name, "budget_eur": budget_eur},
                old_values={},
                new_values={},
                success=False,
                error_message=error.message,
            )
        return format_error_for_llm(error)
