"""W13: Set bid adjustments by device or location."""

import logging
import ads_mcp.utils as utils
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver
from tools.validation import validate_mode, validate_numeric_range, validate_enum
from tools.error_handler import handle_google_ads_error, handle_validation_error, format_error_for_llm
from tools.mutation import MutationPreview, MutationResult, format_preview_for_llm, format_result_for_llm
from tools.audit import get_audit_logger
from tools.name_resolver import resolve_campaign
from google.protobuf import field_mask_pb2
from google.ads.googleads.errors import GoogleAdsException

logger = logging.getLogger(__name__)


@mcp.tool()
def set_bid_adjustments(
    client: str,
    campaign: str,
    dimension: str,
    criterion: str,
    modifier: float,
    mode: str = "preview",
) -> str:
    """Set bid adjustments by device or location.

    USE THIS TOOL WHEN:
    - User wants to adjust bids for specific devices or locations
    - "aumenta bid per mobile", "riduci bid per desktop"

    ALWAYS call with mode="preview" first.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID.
        dimension: DEVICE or LOCATION.
        criterion: For DEVICE: MOBILE, DESKTOP, TABLET. For LOCATION: location criterion ID.
        modifier: Bid modifier (-0.90 to 10.0, e.g., -0.20 for -20%, 0.50 for +50%).
        mode: "preview" or "execute". Default preview.
    """
    if not validate_mode(mode):
        return format_error_for_llm(handle_validation_error("mode must be 'preview' or 'execute'", "mode"))
    if not validate_enum(dimension, ["DEVICE", "LOCATION"]):
        return format_error_for_llm(handle_validation_error("dimension must be DEVICE or LOCATION", "dimension"))
    if not validate_numeric_range(modifier, -0.90, 10.0):
        return format_error_for_llm(handle_validation_error("modifier must be -0.90 to 10.0", "modifier"))

    try:
        customer_id, campaign_id = resolve_campaign(client, campaign)
        client_name = ClientResolver.resolve_name(customer_id)
    except ValueError as e:
        return format_error_for_llm(handle_validation_error(str(e)))

    pct_change = modifier * 100
    preview = MutationPreview(
        tool_name="set_bid_adjustments",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Set {dimension} bid adjustment: {criterion}",
        changes=[{"field": "Bid Modifier", "old": "—", "new": f"{pct_change:+.0f}%"}],
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    try:
        svc = utils.get_googleads_service("CampaignCriterionService")
        op = utils.get_googleads_type("CampaignCriterionOperation")

        if dimension.upper() == "DEVICE":
            op.create.campaign = svc.campaign_path(customer_id, campaign_id)
            op.create.device.type_ = utils._googleads_client.enums.DeviceEnum.Device[criterion.upper()]
            op.create.bid_modifier = modifier
        else:  # LOCATION
            op.create.campaign = svc.campaign_path(customer_id, campaign_id)
            op.create.location.geo_target_constant = f"geo_target_constants/{criterion}"
            op.create.bid_modifier = modifier

        response = svc.mutate_campaign_criteria(customer_id=customer_id, operations=[op])

        result = MutationResult(
            success=True,
            message=f"Bid adjustment set for {dimension} '{criterion}' to {pct_change:+.0f}%.",
        )

        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="set_bid_adjustments",
                action="set_adjustment",
                parameters={"dimension": dimension, "criterion": criterion, "modifier": modifier},
                old_values={},
                new_values={"modifier": modifier},
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
                tool_name="set_bid_adjustments",
                action="set_adjustment",
                parameters={},
                old_values={},
                new_values={},
                success=False,
                error_message=error.message,
            )
        return format_error_for_llm(error)
