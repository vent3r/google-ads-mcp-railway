"""W6: Enable or pause a keyword."""

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
from tools.name_resolver import resolve_campaign, resolve_adgroup, resolve_keyword
from google.protobuf import field_mask_pb2
from google.ads.googleads.errors import GoogleAdsException

logger = logging.getLogger(__name__)


def _get_ads_client():
    from ads_mcp.coordinator import get_google_ads_client
    return get_google_ads_client()


@mcp.tool()
def set_keyword_status(
    client: str,
    campaign: str,
    adgroup: str,
    keyword: str,
    status: str,
    mode: str = "preview",
) -> str:
    """Enable or pause a keyword.

    USE THIS TOOL WHEN:
    - User wants to pause or enable a keyword
    - "pausa questa keyword", "attiva la keyword"

    ALWAYS call with mode="preview" first.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID.
        adgroup: Ad group name or ID.
        keyword: Keyword text or criterion ID.
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
        criterion_id = resolve_keyword(customer_id, adgroup_id, keyword)
        client_name = ClientResolver.resolve_name(customer_id)
    except ValueError as e:
        return format_error_for_llm(handle_validation_error(str(e)))

    try:
        q = (
            f"SELECT ad_group_criterion.criterion_id, ad_group_criterion.keyword.text, "
            f"ad_group_criterion.status FROM ad_group_criterion "
            f"WHERE ad_group_criterion.criterion_id = {criterion_id} LIMIT 1"
        )
        rows = run_query(customer_id, q)
        if not rows:
            return format_error_for_llm(
                handle_validation_error(f"Keyword ID {criterion_id} not found")
            )
        current = rows[0]
        old_status = current.get("ad_group_criterion.status", "UNKNOWN")
        kw_text = current.get("ad_group_criterion.keyword.text", keyword)
    except GoogleAdsException as ex:
        return format_error_for_llm(handle_google_ads_error(ex))

    if old_status.upper() == status.upper():
        return f"ℹ️ Keyword '{kw_text}' is already {status}. No changes needed."

    preview = MutationPreview(
        tool_name="set_keyword_status",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Set keyword status: {kw_text}",
        changes=[{"field": "Status", "old": old_status, "new": status.upper()}],
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    try:
        ads_client = _get_ads_client()
        svc = ads_client.get_service("AdGroupCriterionService")
        op = ads_client.get_type("AdGroupCriterionOperation")
        op.update.resource_name = svc.ad_group_criterion_path(
            customer_id, adgroup_id, criterion_id
        )
        op.update.status = ads_client.enums.AdGroupCriterionStatusEnum.AdGroupCriterionStatus[
            status.upper()
        ]
        op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
        svc.mutate_ad_group_criteria(customer_id=customer_id, operations=[op])

        result = MutationResult(
            success=True,
            resource_id=criterion_id,
            message=f"Keyword '{kw_text}' status: {old_status} → {status.upper()}",
        )

        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="set_keyword_status",
                action="set_status",
                parameters={"keyword": keyword, "status": status},
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
                tool_name="set_keyword_status",
                action="set_status",
                parameters={"keyword": keyword, "status": status},
                old_values={"status": old_status},
                new_values={"status": status},
                success=False,
                error_message=error.message,
            )
        return format_error_for_llm(error)
