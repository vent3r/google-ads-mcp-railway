"""W11: Create a responsive search ad (RSA)."""

import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver
from tools.validation import validate_mode, validate_headline, validate_description, validate_url
from tools.error_handler import handle_google_ads_error, handle_validation_error, format_error_for_llm
from tools.mutation import MutationPreview, MutationResult, format_preview_for_llm, format_result_for_llm
from tools.audit import get_audit_logger
from tools.name_resolver import resolve_campaign, resolve_adgroup
from google.ads.googleads.errors import GoogleAdsException

logger = logging.getLogger(__name__)


def _get_ads_client():
    from ads_mcp.coordinator import get_google_ads_client
    return get_google_ads_client()


@mcp.tool()
def create_rsa(
    client: str,
    campaign: str,
    adgroup: str,
    headlines: str,
    descriptions: str,
    final_url: str,
    path1: str = "",
    path2: str = "",
    mode: str = "preview",
) -> str:
    """Create a responsive search ad (RSA).

    USE THIS TOOL WHEN:
    - User wants to create a new responsive search ad
    - "crea un nuovo annuncio", "new responsive ad"

    ALWAYS call with mode="preview" first.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID.
        adgroup: Ad group name or ID.
        headlines: Comma-separated headlines (min 3, max 15, max 30 chars each).
        descriptions: Comma-separated descriptions (min 2, max 4, max 90 chars each).
        final_url: Landing page URL (must be https://).
        path1: Optional URL path (optional, max 15 chars).
        path2: Optional URL path (optional, max 15 chars).
        mode: "preview" or "execute". Default preview.
    """
    if not validate_mode(mode):
        return format_error_for_llm(handle_validation_error("mode must be 'preview' or 'execute'", "mode"))
    if not validate_url(final_url):
        return format_error_for_llm(handle_validation_error("Invalid URL", "final_url"))

    headlines_list = [h.strip() for h in headlines.split(",") if h.strip()]
    descriptions_list = [d.strip() for d in descriptions.split(",") if d.strip()]

    if len(headlines_list) < 3 or len(headlines_list) > 15:
        return format_error_for_llm(handle_validation_error("Need 3-15 headlines", "headlines"))
    if len(descriptions_list) < 2 or len(descriptions_list) > 4:
        return format_error_for_llm(handle_validation_error("Need 2-4 descriptions", "descriptions"))

    for h in headlines_list:
        if not validate_headline(h):
            return format_error_for_llm(handle_validation_error(f"Headline too long: '{h}'", "headlines"))
    for d in descriptions_list:
        if not validate_description(d):
            return format_error_for_llm(handle_validation_error(f"Description too long: '{d}'", "descriptions"))

    try:
        customer_id, campaign_id = resolve_campaign(client, campaign)
        adgroup_id = resolve_adgroup(customer_id, campaign_id, adgroup)
        client_name = ClientResolver.resolve_name(customer_id)
    except ValueError as e:
        return format_error_for_llm(handle_validation_error(str(e)))

    preview = MutationPreview(
        tool_name="create_rsa",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Create RSA in ad group",
        changes=[
            {"field": "Headlines", "old": "—", "new": f"{len(headlines_list)} headlines"},
            {"field": "Descriptions", "old": "—", "new": f"{len(descriptions_list)} descriptions"},
            {"field": "Final URL", "old": "—", "new": final_url},
        ],
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    try:
        ads_client = _get_ads_client()
        svc = ads_client.get_service("AdGroupAdService")
        op = ads_client.get_type("AdGroupAdOperation")
        ad = op.create.ad
        ad.final_urls.append(final_url)
        if path1:
            ad.final_url_path1 = path1
        if path2:
            ad.final_url_path2 = path2

        rsa = ad.responsive_search_ad
        for h in headlines_list:
            headline = rsa.headlines.add()
            headline.text = h
        for d in descriptions_list:
            desc = rsa.descriptions.add()
            desc.text = d

        op.create.ad_group = svc.ad_group_path(customer_id, adgroup_id)

        response = svc.mutate_ad_group_ads(customer_id=customer_id, operations=[op])

        result = MutationResult(
            success=True,
            resource_id=response.results[0].resource_name.split("/")[-1],
            message=f"RSA created with {len(headlines_list)} headlines and {len(descriptions_list)} descriptions.",
        )

        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="create_rsa",
                action="create",
                parameters={"adgroup": adgroup, "headlines_count": len(headlines_list)},
                old_values={},
                new_values={"ad_id": result.resource_id},
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
                tool_name="create_rsa",
                action="create",
                parameters={},
                old_values={},
                new_values={},
                success=False,
                error_message=error.message,
            )
        return format_error_for_llm(error)
