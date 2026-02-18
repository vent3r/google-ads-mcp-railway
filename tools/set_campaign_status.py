"""W2: Enable or pause a campaign."""

import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, run_query
from tools.validation import validate_mode, validate_enum
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
    from ads_mcp.coordinator import get_google_ads_client

    return get_google_ads_client()


@mcp.tool()
def set_campaign_status(
    client: str,
    campaign: str,
    status: str,
    mode: str = "preview",
) -> str:
    """Enable or pause a campaign.

    USE THIS TOOL WHEN:
    - User asks to pause or enable a campaign
    - "pausa la campagna", "attiva la campagna", "ferma la campagna"

    ALWAYS call with mode="preview" first.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID.
        status: ENABLED or PAUSED.
        mode: "preview" or "execute". Default preview.
    """
    if not validate_mode(mode):
        return format_error_for_llm(
            handle_validation_error("mode must be 'preview' or 'execute'", "mode")
        )
    if not validate_enum(status, ["ENABLED", "PAUSED"]):
        return format_error_for_llm(
            handle_validation_error("status must be ENABLED or PAUSED", "status")
        )

    try:
        customer_id, campaign_id = resolve_campaign(client, campaign)
        client_name = ClientResolver.resolve_name(customer_id)
    except ValueError as e:
        return format_error_for_llm(handle_validation_error(str(e)))

    try:
        q = (
            f"SELECT campaign.id, campaign.name, campaign.status "
            f"FROM campaign WHERE campaign.id = {campaign_id} LIMIT 1"
        )
        rows = run_query(customer_id, q)
        if not rows:
            return format_error_for_llm(
                handle_validation_error(f"Campaign {campaign_id} not found")
            )
        current = rows[0]
        old_status = current.get("campaign.status", "UNKNOWN")
        campaign_name = current.get("campaign.name", campaign)
    except GoogleAdsException as ex:
        return format_error_for_llm(handle_google_ads_error(ex))

    if old_status.upper() == status.upper():
        return f"ℹ️ Campaign '{campaign_name}' is already {status}. No changes needed."

    warnings = []
    if status.upper() == "PAUSED":
        warnings.append(f"Campaign '{campaign_name}' will stop serving ads immediately.")

    preview = MutationPreview(
        tool_name="set_campaign_status",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Set campaign status: {campaign_name}",
        changes=[{"field": "Status", "old": old_status, "new": status.upper()}],
        warnings=warnings,
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    try:
        ads_client = _get_ads_client()
        svc = ads_client.get_service("CampaignService")
        op = ads_client.get_type("CampaignOperation")
        op.update.resource_name = svc.campaign_path(customer_id, campaign_id)
        op.update.status = ads_client.enums.CampaignStatusEnum.CampaignStatus[
            status.upper()
        ]
        op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
        svc.mutate_campaigns(customer_id=customer_id, operations=[op])

        result = MutationResult(
            success=True,
            resource_id=campaign_id,
            message=f"Campaign '{campaign_name}' status: {old_status} → {status.upper()}",
        )

        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="set_campaign_status",
                action="set_status",
                parameters={"campaign": campaign, "status": status},
                old_values={"status": old_status},
                new_values={"status": status.upper()},
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
                tool_name="set_campaign_status",
                action="set_status",
                parameters={"campaign": campaign, "status": status},
                old_values={"status": old_status},
                new_values={"status": status},
                success=False,
                error_message=error.message,
            )
        return format_error_for_llm(error)
