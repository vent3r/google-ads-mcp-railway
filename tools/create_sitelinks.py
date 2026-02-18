"""W14: Create sitelink extensions."""

import logging
import json
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver
from tools.validation import validate_mode, validate_url, validate_string_length
from tools.error_handler import handle_google_ads_error, handle_validation_error, format_error_for_llm
from tools.mutation import MutationPreview, MutationResult, format_preview_for_llm, format_result_for_llm
from tools.audit import get_audit_logger
from tools.name_resolver import resolve_campaign
from google.ads.googleads.errors import GoogleAdsException

logger = logging.getLogger(__name__)


def _get_ads_client():
    from ads_mcp.coordinator import get_google_ads_client
    return get_google_ads_client()


@mcp.tool()
def create_sitelinks(
    client: str,
    campaign: str,
    sitelinks: str,
    mode: str = "preview",
) -> str:
    """Create sitelink extensions for a campaign.

    USE THIS TOOL WHEN:
    - User wants to add sitelinks to a campaign
    - "aggiungi sitelink", "nuovi link"

    ALWAYS call with mode="preview" first.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID.
        sitelinks: JSON string with array of sitelinks. Each has: text, final_url, description1 (opt), description2 (opt).
        mode: "preview" or "execute". Default preview.
    """
    if not validate_mode(mode):
        return format_error_for_llm(handle_validation_error("mode must be 'preview' or 'execute'", "mode"))

    try:
        sitelinks_list = json.loads(sitelinks)
        if not isinstance(sitelinks_list, list):
            raise ValueError("sitelinks must be a JSON array")
    except (json.JSONDecodeError, ValueError) as e:
        return format_error_for_llm(handle_validation_error(f"Invalid JSON: {str(e)}", "sitelinks"))

    if not (2 <= len(sitelinks_list) <= 20):
        return format_error_for_llm(handle_validation_error("Need 2-20 sitelinks", "sitelinks"))

    for sl in sitelinks_list:
        if not validate_string_length(sl.get("text", ""), 1, 25):
            return format_error_for_llm(handle_validation_error("Sitelink text must be 1-25 chars", "sitelinks"))
        if not validate_url(sl.get("final_url", "")):
            return format_error_for_llm(handle_validation_error("Invalid sitelink URL", "sitelinks"))

    try:
        customer_id, campaign_id = resolve_campaign(client, campaign)
        client_name = ClientResolver.resolve_name(customer_id)
    except ValueError as e:
        return format_error_for_llm(handle_validation_error(str(e)))

    preview = MutationPreview(
        tool_name="create_sitelinks",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Create {len(sitelinks_list)} sitelinks",
        changes=[{"field": "Sitelinks", "old": "—", "new": f"{len(sitelinks_list)} sitelinks added"}],
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    try:
        ads_client = _get_ads_client()
        asset_svc = ads_client.get_service("AssetService")
        campaign_asset_svc = ads_client.get_service("CampaignAssetService")

        asset_ids = []
        for sl in sitelinks_list:
            asset_op = ads_client.get_type("AssetOperation")
            sitelink = asset_op.create.sitelink_asset
            sitelink.link_text = sl.get("text", "")
            sitelink.final_urls.append(sl.get("final_url", ""))
            if sl.get("description1"):
                sitelink.description1 = sl.get("description1")
            if sl.get("description2"):
                sitelink.description2 = sl.get("description2")

            asset_response = asset_svc.create_assets(customer_id=customer_id, operations=[asset_op])
            asset_ids.append(asset_response.results[0].resource_name)

        ca_ops = []
        for asset_name in asset_ids:
            ca_op = ads_client.get_type("CampaignAssetOperation")
            ca_op.create.asset = asset_name
            ca_op.create.campaign = campaign_asset_svc.campaign_path(customer_id, campaign_id)
            ca_op.create.field_type = ads_client.enums.AssetFieldTypeEnum.AssetFieldType.SITELINK
            ca_ops.append(ca_op)

        ca_response = campaign_asset_svc.mutate_campaign_assets(
            customer_id=customer_id, operations=ca_ops
        )

        result = MutationResult(
            success=True,
            message=f"Created {len(sitelinks_list)} sitelinks.",
        )

        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="create_sitelinks",
                action="create",
                parameters={"sitelinks_count": len(sitelinks_list)},
                old_values={},
                new_values={"asset_ids": asset_ids},
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
                tool_name="create_sitelinks",
                action="create",
                parameters={},
                old_values={},
                new_values={},
                success=False,
                error_message=error.message,
            )
        return format_error_for_llm(error)
