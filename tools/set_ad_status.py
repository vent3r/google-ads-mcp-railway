"""W12: Enable or pause an ad."""

import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver
from tools.validation import validate_mode, validate_enum
from tools.error_handler import handle_google_ads_error, handle_validation_error, format_error_for_llm
from tools.mutation import MutationPreview, MutationResult, format_preview_for_llm, format_result_for_llm
from tools.audit import get_audit_logger
from tools.name_resolver import resolve_campaign, resolve_adgroup
from google.protobuf import field_mask_pb2
from google.ads.googleads.errors import GoogleAdsException

logger = logging.getLogger(__name__)


def _get_ads_client():
    from ads_mcp.coordinator import get_google_ads_client
    return get_google_ads_client()


@mcp.tool()
def set_ad_status(
    client: str,
    campaign: str,
    adgroup: str,
    ad_id: str,
    status: str,
    mode: str = "preview",
) -> str:
    """Enable or pause an ad.

    USE THIS TOOL WHEN:
    - User wants to pause or enable an individual ad
    - "pausa questo annuncio", "attiva l'ad"

    ALWAYS call with mode="preview" first.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID.
        adgroup: Ad group name or ID.
        ad_id: Numeric ad ID.
        status: ENABLED or PAUSED.
        mode: "preview" or "execute". Default preview.
    """
    if not validate_mode(mode):
        return format_error_for_llm(handle_validation_error("mode must be 'preview' or 'execute'", "mode"))
    if not validate_enum(status, ["ENABLED", "PAUSED"]):
        return format_error_for_llm(handle_validation_error("status must be ENABLED or PAUSED", "status"))

    try:
        customer_id, campaign_id = resolve_campaign(client, campaign)
        adgroup_id = resolve_adgroup(customer_id, campaign_id, adgroup)
        client_name = ClientResolver.resolve_name(customer_id)
    except ValueError as e:
        return format_error_for_llm(handle_validation_error(str(e)))

    preview = MutationPreview(
        tool_name="set_ad_status",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Set ad status: {ad_id}",
        changes=[{"field": "Status", "old": "CURRENT", "new": status.upper()}],
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    try:
        ads_client = _get_ads_client()
        svc = ads_client.get_service("AdGroupAdService")
        op = ads_client.get_type("AdGroupAdOperation")
        op.update.resource_name = svc.ad_group_ad_path(customer_id, adgroup_id, ad_id)
        op.update.status = ads_client.enums.AdGroupAdStatusEnum.AdGroupAdStatus[status.upper()]
        op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
        svc.mutate_ad_group_ads(customer_id=customer_id, operations=[op])

        result = MutationResult(
            success=True,
            resource_id=ad_id,
            message=f"Ad {ad_id} status changed to {status.upper()}.",
        )

        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="set_ad_status",
                action="set_status",
                parameters={"ad_id": ad_id, "status": status},
                old_values={},
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
                tool_name="set_ad_status",
                action="set_status",
                parameters={"ad_id": ad_id, "status": status},
                old_values={},
                new_values={},
                success=False,
                error_message=error.message,
            )
        return format_error_for_llm(error)
