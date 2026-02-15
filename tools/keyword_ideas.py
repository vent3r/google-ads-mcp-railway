"""Tool 11: keyword_ideas — Generate keyword ideas via KeywordPlanIdeaService.

Requires Standard Access on the MCC developer token.
Default targeting: Italy (2380), Italian (1014).
"""

import logging

import ads_mcp.utils as utils
from ads_mcp.coordinator import mcp
from tools.helpers import (
    ClientResolver,
    QuotaTracker,
)
from tools.options import (
    build_header,
    format_output,
)

logger = logging.getLogger(__name__)

# Common location IDs
LOCATION_IDS = {
    "IT": "2380", "US": "2840", "UK": "2826",
    "DE": "2276", "FR": "2250", "ES": "2724",
}

# Common language IDs
LANGUAGE_IDS = {
    "it": "1014", "en": "1000", "de": "1001",
    "fr": "1002", "es": "1003",
}


@mcp.tool()
def keyword_ideas(
    client: str,
    keywords: str,
    page_url: str = "",
    country: str = "IT",
    language: str = "it",
    max_results: int = 50,
    include_adult: bool = False,
) -> str:
    """Generate keyword ideas with search volume and competition data.

    Uses Google Ads Keyword Planner (requires Standard Access).
    Provide seed keywords and/or a page URL to discover new keyword opportunities.

    Args:
        client: Account name or customer ID.
        keywords: Comma-separated seed keywords (e.g. "spedizioni, corriere espresso").
        page_url: Optional URL to seed ideas from (e.g. "https://www.spedire.com").
        country: Country code for targeting — IT, US, UK, DE, FR, ES (default IT).
        language: Language code — it, en, de, fr, es (default it).
        max_results: Maximum keyword ideas to return (default 50, max 200).
        include_adult: Include adult keywords (default false).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    # Parse keywords
    keyword_list = [k.strip() for k in keywords.split(",") if k.strip()]

    if not keyword_list and not page_url:
        return "Error: provide at least one keyword or a page_url."

    # Resolve location and language
    location_id = LOCATION_IDS.get(country.upper(), LOCATION_IDS["IT"])
    language_id = LANGUAGE_IDS.get(language.lower(), LANGUAGE_IDS["it"])

    max_results = min(max_results, 200)

    try:
        kp_service = utils.get_googleads_service("KeywordPlanIdeaService")
        ga_service = utils.get_googleads_service("GoogleAdsService")

        QuotaTracker.increment()

        # Build request
        request = utils.get_googleads_type("GenerateKeywordIdeasRequest")
        request.customer_id = customer_id
        request.language = ga_service.language_constant_path(language_id)
        request.geo_target_constants.append(
            ga_service.geo_target_constant_path(location_id)
        )
        request.include_adult_keywords = include_adult
        request.keyword_plan_network = (
            utils.get_googleads_type("KeywordPlanNetworkEnum")
            .KeywordPlanNetwork.GOOGLE_SEARCH_AND_PARTNERS
        )
        request.page_size = max_results

        # Set seed
        if keyword_list and not page_url:
            request.keyword_seed.keywords.extend(keyword_list)
        elif not keyword_list and page_url:
            request.url_seed.url = page_url
        else:
            request.keyword_and_url_seed.url = page_url
            request.keyword_and_url_seed.keywords.extend(keyword_list)

        # Execute
        response = kp_service.generate_keyword_ideas(request=request)

        # Process results
        results = []
        for idea in response:
            m = idea.keyword_idea_metrics
            avg_searches = m.avg_monthly_searches if m.avg_monthly_searches else 0
            competition = m.competition.name if m.competition else "-"
            comp_index = m.competition_index if m.competition_index else 0
            low_bid = m.low_top_of_page_bid_micros / 1_000_000 if m.low_top_of_page_bid_micros else 0
            high_bid = m.high_top_of_page_bid_micros / 1_000_000 if m.high_top_of_page_bid_micros else 0

            results.append({
                "keyword": idea.text,
                "avg_searches": f"{avg_searches:,}",
                "competition": competition,
                "comp_idx": str(comp_index),
                "low_bid": f"{low_bid:,.2f}",
                "high_bid": f"{high_bid:,.2f}",
                "_sort": avg_searches,
            })

            if len(results) >= max_results:
                break

        if not results:
            return (
                f"No keyword ideas found.\n"
                f"Seeds: {', '.join(keyword_list) if keyword_list else 'none'}\n"
                f"URL: {page_url or 'none'}\n"
                f"Country: {country}, Language: {language}"
            )

        # Sort by search volume descending
        results.sort(key=lambda r: r["_sort"], reverse=True)
        for r in results:
            r.pop("_sort")

        columns = [
            ("keyword", "Keyword"),
            ("avg_searches", "Avg Monthly"),
            ("competition", "Competition"),
            ("comp_idx", "Comp Idx"),
            ("low_bid", "Low Bid €"),
            ("high_bid", "High Bid €"),
        ]

        seeds_str = ", ".join(keyword_list) if keyword_list else "none"
        header = build_header(
            title="Keyword Ideas",
            client_name=client_name,
            extra=f"{len(results)} results · Seeds: {seeds_str}"
                  f"{f' · URL: {page_url}' if page_url else ''}"
                  f" · {country.upper()}/{language}",
        )

        return format_output(results, columns, header=header)

    except Exception as e:
        error_msg = str(e)
        if "PERMISSION_DENIED" in error_msg or "NOT_ALLOWED" in error_msg:
            return (
                "Error: Keyword Planner access denied. "
                "This requires Standard Access on the MCC developer token. "
                f"Details: {error_msg[:200]}"
            )
        return f"Error generating keyword ideas: {error_msg[:300]}"
