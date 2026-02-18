"""W4: Add negative keywords to campaign or ad group level."""

import logging
import ads_mcp.utils as utils
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, run_query
from tools.validation import (
    validate_mode,
    validate_match_type,
    validate_keyword_text,
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
from tools.name_resolver import resolve_campaign, resolve_adgroup
from google.ads.googleads.errors import GoogleAdsException

logger = logging.getLogger(__name__)


@mcp.tool()
def add_negatives(
    client: str,
    keywords: str,
    campaign: str,
    adgroup: str = "",
    match_type: str = "PHRASE",
    mode: str = "preview",
) -> str:
    """Add negative keywords to a campaign or ad group.

    USE THIS TOOL WHEN:
    - User wants to block specific search terms
    - After suggest_negatives or search_term_analysis reveals wasteful terms
    - "aggiungi negative", "blocca queste keyword", "negativizza"

    ALWAYS call with mode="preview" first.

    Args:
        client: Account name or customer ID.
        keywords: Comma-separated list of keywords to add as negatives.
        campaign: Campaign name or ID (required).
        adgroup: Ad group name or ID (optional — if empty, adds at campaign level).
        match_type: BROAD, PHRASE (default), or EXACT.
        mode: "preview" or "execute". Default preview.
    """
    if not validate_mode(mode):
        return format_error_for_llm(
            handle_validation_error("mode must be 'preview' or 'execute'", "mode")
        )
    if not validate_match_type(match_type):
        return format_error_for_llm(
            handle_validation_error(
                "match_type must be BROAD, PHRASE, or EXACT", "match_type"
            )
        )

    # Parse keywords
    kw_list = [kw.strip() for kw in keywords.split(",") if kw.strip()]
    if not kw_list:
        return format_error_for_llm(handle_validation_error("No keywords provided", "keywords"))
    if len(kw_list) > 50:
        return format_error_for_llm(
            handle_validation_error("Max 50 keywords per call", "keywords")
        )

    for kw in kw_list:
        if not validate_keyword_text(kw):
            return format_error_for_llm(
                handle_validation_error(
                    f"Invalid keyword: '{kw}' (1-80 chars required)", "keywords"
                )
            )

    # Resolve names
    try:
        customer_id, campaign_id = resolve_campaign(client, campaign)
        client_name = ClientResolver.resolve_name(customer_id)
        adgroup_id = None
        if adgroup:
            adgroup_id = resolve_adgroup(customer_id, campaign_id, adgroup)
    except ValueError as e:
        return format_error_for_llm(handle_validation_error(str(e)))

    level = "ad group" if adgroup_id else "campaign"

    preview = MutationPreview(
        tool_name="add_negatives",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Add {len(kw_list)} negative keywords ({level} level)",
        changes=[{"field": f"Negative [{match_type.upper()}]", "old": "—", "new": kw} for kw in kw_list],
        warnings=[f"Adding {len(kw_list)} keywords at {level} level"],
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    # Execute
    try:
        if adgroup_id:
            # Ad group level negatives
            svc = utils.get_googleads_service("AdGroupCriterionService")
            operations = []
            for kw in kw_list:
                op = utils.get_googleads_type("AdGroupCriterionOperation")
                criterion = op.create
                criterion.ad_group = svc.ad_group_path(customer_id, adgroup_id)
                criterion.negative = True
                criterion.keyword.text = kw
                criterion.keyword.match_type = (
                    utils._googleads_client.enums.KeywordMatchTypeEnum.KeywordMatchType[
                        match_type.upper()
                    ]
                )
                operations.append(op)
            response = svc.mutate_ad_group_criteria(
                customer_id=customer_id, operations=operations
            )
        else:
            # Campaign level negatives
            svc = utils.get_googleads_service("CampaignCriterionService")
            operations = []
            for kw in kw_list:
                op = utils.get_googleads_type("CampaignCriterionOperation")
                criterion = op.create
                criterion.campaign = svc.campaign_path(customer_id, campaign_id)
                criterion.negative = True
                criterion.keyword.text = kw
                criterion.keyword.match_type = (
                    utils._googleads_client.enums.KeywordMatchTypeEnum.KeywordMatchType[
                        match_type.upper()
                    ]
                )
                operations.append(op)
            response = svc.mutate_campaign_criteria(
                customer_id=customer_id, operations=operations
            )

        result = MutationResult(
            success=True,
            message=f"Added {len(kw_list)} negative keywords [{match_type.upper()}] at {level} level.",
        )

        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="add_negatives",
                action="add_negatives",
                parameters={"keywords": kw_list, "match_type": match_type, "level": level},
                old_values={},
                new_values={"keywords_added": kw_list},
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
                tool_name="add_negatives",
                action="add_negatives",
                parameters={"keywords": kw_list, "match_type": match_type},
                old_values={},
                new_values={},
                success=False,
                error_message=error.message,
            )
        return format_error_for_llm(error)
