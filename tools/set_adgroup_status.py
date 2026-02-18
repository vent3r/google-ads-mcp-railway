"""W3: Enable or pause an ad group."""

import logging
import ads_mcp.utils as utils
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
from tools.name_resolver import resolve_campaign, resolve_adgroup
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf import field_mask_pb2

logger = logging.getLogger(__name__)


@mcp.tool()
def set_adgroup_status(
    client: str,
    campaign: str,
    adgroup: str,
    status: str,
    mode: str = "preview",
) -> str:
    """Enable or pause an ad group.

    USE THIS TOOL WHEN:
    - User asks to pause or enable an ad group within a campaign
    - "pausa l'ad group", "attiva il gruppo di annunci"

    ALWAYS call with mode="preview" first.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID.
        adgroup: Ad group name or ID.
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
        adgroup_id = resolve_adgroup(customer_id, campaign_id, adgroup)
        client_name = ClientResolver.resolve_name(customer_id)
    except ValueError as e:
        return format_error_for_llm(handle_validation_error(str(e)))

    try:
        q = (
            f"SELECT ad_group.id, ad_group.name, ad_group.status "
            f"FROM ad_group WHERE ad_group.id = {adgroup_id} LIMIT 1"
        )
        rows = run_query(customer_id, q)
        if not rows:
            return format_error_for_llm(
                handle_validation_error(f"Ad group {adgroup_id} not found")
            )
        current = rows[0]
        old_status = current.get("ad_group.status", "UNKNOWN")
        adgroup_name = current.get("ad_group.name", adgroup)
    except GoogleAdsException as ex:
        return format_error_for_llm(handle_google_ads_error(ex))

    if old_status.upper() == status.upper():
        return f"ℹ️ Ad group '{adgroup_name}' is already {status}. No changes needed."

    warnings = []
    if status.upper() == "PAUSED":
        warnings.append(f"Ad group '{adgroup_name}' will stop serving ads immediately.")

    preview = MutationPreview(
        tool_name="set_adgroup_status",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Set ad group status: {adgroup_name}",
        changes=[{"field": "Status", "old": old_status, "new": status.upper()}],
        warnings=warnings,
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    try:
        svc = utils.get_googleads_service("AdGroupService")
        op = utils.get_googleads_type("AdGroupOperation")
        op.update.resource_name = svc.ad_group_path(customer_id, adgroup_id)
        op.update.status = utils._googleads_client.enums.AdGroupStatusEnum.AdGroupStatus[
            status.upper()
        ]
        op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
        svc.mutate_ad_groups(customer_id=customer_id, operations=[op])

        result = MutationResult(
            success=True,
            resource_id=adgroup_id,
            message=f"Ad group '{adgroup_name}' status: {old_status} → {status.upper()}",
        )

        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="set_adgroup_status",
                action="set_status",
                parameters={"campaign": campaign, "adgroup": adgroup, "status": status},
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
                tool_name="set_adgroup_status",
                action="set_status",
                parameters={"campaign": campaign, "adgroup": adgroup, "status": status},
                old_values={"status": old_status},
                new_values={"status": status},
                success=False,
                error_message=error.message,
            )
        return format_error_for_llm(error)
