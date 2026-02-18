"""W10: Add keywords to an ad group."""

import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver
from tools.validation import (
    validate_mode,
    validate_keyword_text,
    validate_match_type,
    validate_bid_amount,
    euros_to_micros,
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


def _get_ads_client():
    from ads_mcp.coordinator import get_google_ads_client
    return get_google_ads_client()


@mcp.tool()
def add_keywords(
    client: str,
    campaign: str,
    adgroup: str,
    keywords: str,
    match_type: str = "BROAD",
    bid_eur: float = 0.0,
    mode: str = "preview",
) -> str:
    """Add keywords to an ad group.

    USE THIS TOOL WHEN:
    - User wants to add new keywords
    - "aggiungi keyword", "nuove parole chiave"

    ALWAYS call with mode="preview" first.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID.
        adgroup: Ad group name or ID.
        keywords: Comma-separated keyword texts. Max 50 per call.
        match_type: BROAD (default), PHRASE, or EXACT.
        bid_eur: Optional keyword-level bid (0.01-100.00). If 0, uses ad group default.
        mode: "preview" or "execute". Default preview.
    """
    if not validate_mode(mode):
        return format_error_for_llm(handle_validation_error("mode must be 'preview' or 'execute'", "mode"))
    if not validate_match_type(match_type):
        return format_error_for_llm(handle_validation_error("Invalid match_type", "match_type"))
    if bid_eur > 0 and not validate_bid_amount(bid_eur):
        return format_error_for_llm(handle_validation_error("Bid out of range", "bid_eur"))

    kw_list = [kw.strip() for kw in keywords.split(",") if kw.strip()]
    if not kw_list:
        return format_error_for_llm(handle_validation_error("No keywords provided", "keywords"))
    if len(kw_list) > 50:
        return format_error_for_llm(handle_validation_error("Max 50 keywords per call", "keywords"))

    for kw in kw_list:
        if not validate_keyword_text(kw):
            return format_error_for_llm(handle_validation_error(f"Invalid keyword: '{kw}'", "keywords"))

    try:
        customer_id, campaign_id = resolve_campaign(client, campaign)
        adgroup_id = resolve_adgroup(customer_id, campaign_id, adgroup)
        client_name = ClientResolver.resolve_name(customer_id)
    except ValueError as e:
        return format_error_for_llm(handle_validation_error(str(e)))

    preview = MutationPreview(
        tool_name="add_keywords",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Add {len(kw_list)} keywords [{match_type}] to ad group",
        changes=[{"field": f"Keyword [{match_type}]", "old": "—", "new": kw} for kw in kw_list],
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    try:
        ads_client = _get_ads_client()
        svc = ads_client.get_service("AdGroupCriterionService")
        operations = []

        for kw in kw_list:
            op = ads_client.get_type("AdGroupCriterionOperation")
            criterion = op.create
            criterion.ad_group = svc.ad_group_path(customer_id, adgroup_id)
            criterion.negative = False
            criterion.keyword.text = kw
            criterion.keyword.match_type = ads_client.enums.KeywordMatchTypeEnum.KeywordMatchType[match_type.upper()]
            if bid_eur > 0:
                criterion.cpc_bid_micros = euros_to_micros(bid_eur)
            operations.append(op)

        response = svc.mutate_ad_group_criteria(customer_id=customer_id, operations=operations)

        result = MutationResult(
            success=True,
            message=f"Added {len(kw_list)} keywords [{match_type}].",
        )

        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="add_keywords",
                action="add",
                parameters={"keywords": kw_list, "match_type": match_type, "bid_eur": bid_eur},
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
                tool_name="add_keywords",
                action="add",
                parameters={"keywords": kw_list, "match_type": match_type},
                old_values={},
                new_values={},
                success=False,
                error_message=error.message,
            )
        return format_error_for_llm(error)
