"""W7: Update keyword CPC bid."""

import logging
import ads_mcp.utils as utils
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver, run_query
from tools.validation import validate_mode, validate_bid_amount, euros_to_micros, micros_to_euros
from tools.error_handler import handle_google_ads_error, handle_validation_error, format_error_for_llm
from tools.mutation import MutationPreview, MutationResult, format_preview_for_llm, format_result_for_llm
from tools.audit import get_audit_logger
from tools.name_resolver import resolve_campaign, resolve_adgroup, resolve_keyword
from google.protobuf import field_mask_pb2
from google.ads.googleads.errors import GoogleAdsException

logger = logging.getLogger(__name__)


@mcp.tool()
def update_keyword_bid(
    client: str,
    campaign: str,
    adgroup: str,
    keyword: str,
    new_bid_eur: float,
    mode: str = "preview",
) -> str:
    """Update keyword CPC bid.

    USE THIS TOOL WHEN:
    - User wants to adjust individual keyword bids
    - "aumenta il bid per questa keyword", "riduci CPC"

    ALWAYS call with mode="preview" first.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID.
        adgroup: Ad group name or ID.
        keyword: Keyword text or criterion ID.
        new_bid_eur: New CPC bid in EUR (min 0.01, max 100.00).
        mode: "preview" or "execute". Default preview.
    """
    if not validate_mode(mode):
        return format_error_for_llm(handle_validation_error("mode must be 'preview' or 'execute'", "mode"))
    if not validate_bid_amount(new_bid_eur):
        return format_error_for_llm(handle_validation_error(f"Bid €{new_bid_eur} out of range (0.01–100.00)", "new_bid_eur"))

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
            f"ad_group_criterion.cpc_bid_micros FROM ad_group_criterion "
            f"WHERE ad_group_criterion.criterion_id = {criterion_id} LIMIT 1"
        )
        rows = run_query(customer_id, q)
        if not rows:
            return format_error_for_llm(handle_validation_error(f"Keyword ID {criterion_id} not found"))
        current = rows[0]
        old_micros = int(current.get("ad_group_criterion.cpc_bid_micros", 0) or 0)
        old_eur = micros_to_euros(old_micros)
        kw_text = current.get("ad_group_criterion.keyword.text", keyword)
    except GoogleAdsException as ex:
        return format_error_for_llm(handle_google_ads_error(ex))

    new_micros = euros_to_micros(new_bid_eur)
    change_pct = ((new_bid_eur - old_eur) / old_eur * 100) if old_eur > 0 else 0

    preview = MutationPreview(
        tool_name="update_keyword_bid",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Update bid: {kw_text}",
        changes=[{"field": "CPC Bid", "old": f"€{old_eur:.3f}", "new": f"€{new_bid_eur:.3f}"}],
        estimated_impact=f"Change: {change_pct:+.1f}%",
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    try:
        svc = utils.get_googleads_service("AdGroupCriterionService")
        op = utils.get_googleads_type("AdGroupCriterionOperation")
        op.update.resource_name = svc.ad_group_criterion_path(customer_id, adgroup_id, criterion_id)
        op.update.cpc_bid_micros = new_micros
        op.update_mask = field_mask_pb2.FieldMask(paths=["cpc_bid_micros"])
        svc.mutate_ad_group_criteria(customer_id=customer_id, operations=[op])

        result = MutationResult(
            success=True,
            resource_id=criterion_id,
            message=f"Keyword '{kw_text}' bid: €{old_eur:.3f} → €{new_bid_eur:.3f} ({change_pct:+.1f}%)",
        )

        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="update_keyword_bid",
                action="update_bid",
                parameters={"keyword": keyword, "new_bid_eur": new_bid_eur},
                old_values={"bid_eur": old_eur},
                new_values={"bid_eur": new_bid_eur},
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
                tool_name="update_keyword_bid",
                action="update_bid",
                parameters={"keyword": keyword, "new_bid_eur": new_bid_eur},
                old_values={"bid_eur": old_eur},
                new_values={"bid_eur": new_bid_eur},
                success=False,
                error_message=error.message,
            )
        return format_error_for_llm(error)
