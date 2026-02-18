"""W5: Remove negative keywords from campaign or ad group level."""

import logging
import ads_mcp.utils as utils
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, run_query
from tools.validation import validate_mode
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

logger = logging.getLogger(__name__)


@mcp.tool()
def remove_negatives(
    client: str,
    keyword_ids: str,
    campaign: str,
    level: str = "campaign",
    adgroup: str = "",
    mode: str = "preview",
) -> str:
    """Remove negative keywords from a campaign or ad group.

    USE THIS TOOL WHEN:
    - User wants to remove previously blocked search terms
    - "rimuovi negative", "sblocca queste keyword"

    ALWAYS call with mode="preview" first.

    Args:
        client: Account name or customer ID.
        keyword_ids: Comma-separated criterion IDs to remove.
        campaign: Campaign name or ID.
        level: "campaign" (default) or "adgroup".
        adgroup: Ad group name or ID (required if level="adgroup").
        mode: "preview" or "execute". Default preview.
    """
    if not validate_mode(mode):
        return format_error_for_llm(
            handle_validation_error("mode must be 'preview' or 'execute'", "mode")
        )

    # Parse IDs
    id_list = [id_str.strip() for id_str in keyword_ids.split(",") if id_str.strip()]
    if not id_list:
        return format_error_for_llm(
            handle_validation_error("No keyword IDs provided", "keyword_ids")
        )
    if len(id_list) > 50:
        return format_error_for_llm(
            handle_validation_error("Max 50 keywords per call", "keyword_ids")
        )

    # Validate IDs are numeric
    for id_str in id_list:
        if not id_str.isdigit():
            return format_error_for_llm(
                handle_validation_error(
                    f"Invalid criterion ID: '{id_str}' (must be numeric)", "keyword_ids"
                )
            )

    # Resolve names
    try:
        customer_id, campaign_id = resolve_campaign(client, campaign)
        client_name = ClientResolver.resolve_name(customer_id)
        adgroup_id = None
        if level.lower() == "adgroup":
            if not adgroup:
                return format_error_for_llm(
                    handle_validation_error(
                        "adgroup is required when level='adgroup'", "adgroup"
                    )
                )
            adgroup_id = resolve_adgroup(customer_id, campaign_id, adgroup)
    except ValueError as e:
        return format_error_for_llm(handle_validation_error(str(e)))

    level_name = "ad group" if adgroup_id else "campaign"

    preview = MutationPreview(
        tool_name="remove_negatives",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Remove {len(id_list)} negative keywords ({level_name} level)",
        changes=[{"field": f"Criterion ID", "old": id_str, "new": "REMOVED"} for id_str in id_list],
        warnings=[f"Removing {len(id_list)} keywords at {level_name} level"],
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    # Execute
    try:
        if adgroup_id:
            # Ad group level
            svc = utils.get_googleads_service("AdGroupCriterionService")
            operations = []
            for id_str in id_list:
                op = utils.get_googleads_type("AdGroupCriterionOperation")
                resource_name = svc.ad_group_criterion_path(
                    customer_id, adgroup_id, id_str
                )
                op.remove = resource_name
                operations.append(op)
            response = svc.mutate_ad_group_criteria(
                customer_id=customer_id, operations=operations
            )
        else:
            # Campaign level
            svc = utils.get_googleads_service("CampaignCriterionService")
            operations = []
            for id_str in id_list:
                op = utils.get_googleads_type("CampaignCriterionOperation")
                resource_name = svc.campaign_criterion_path(customer_id, campaign_id, id_str)
                op.remove = resource_name
                operations.append(op)
            response = svc.mutate_campaign_criteria(
                customer_id=customer_id, operations=operations
            )

        result = MutationResult(
            success=True,
            message=f"Removed {len(id_list)} negative keywords at {level_name} level.",
        )

        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="remove_negatives",
                action="remove_negatives",
                parameters={"keyword_ids": id_list, "level": level_name},
                old_values={"keywords_removed": id_list},
                new_values={},
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
                tool_name="remove_negatives",
                action="remove_negatives",
                parameters={"keyword_ids": id_list, "level": level_name},
                old_values={},
                new_values={},
                success=False,
                error_message=error.message,
            )
        return format_error_for_llm(error)
