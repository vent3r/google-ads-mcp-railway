"""Centralized input validation for all tools.

Every parameter from the LLM is validated BEFORE touching the Google Ads API.
Returns human-readable error messages, not stack traces.
"""

import re
from datetime import datetime, date


def validate_customer_id(customer_id: str) -> bool:
    """Format: 10 digits (with or without hyphens)."""
    clean = customer_id.replace("-", "")
    return bool(re.match(r"^\d{10}$", clean))


def validate_date_format(date_str: str) -> bool:
    """Format: YYYY-MM-DD."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def validate_date_range(start_date: str, end_date: str) -> bool:
    """start <= end, range <= 365 days."""
    try:
        s = datetime.strptime(start_date, "%Y-%m-%d")
        e = datetime.strptime(end_date, "%Y-%m-%d")
        return s <= e and (e - s).days <= 365
    except ValueError:
        return False


def validate_enum(value: str, valid_values: list, case_sensitive: bool = False) -> bool:
    """Value in allowed list."""
    if case_sensitive:
        return value in valid_values
    return value.upper() in [v.upper() for v in valid_values]


def validate_numeric_range(
    value: float, min_value: float = None, max_value: float = None
) -> bool:
    """Numeric range with optional min/max."""
    if min_value is not None and value < min_value:
        return False
    if max_value is not None and value > max_value:
        return False
    return True


def validate_string_length(
    text: str, min_length: int = 0, max_length: int = None
) -> bool:
    """String length validation."""
    if len(text) < min_length:
        return False
    if max_length is not None and len(text) > max_length:
        return False
    return True


def validate_budget_amount(amount_eur: float) -> bool:
    """Budget: >= 1.00 EUR, <= 50000.00 EUR."""
    return validate_numeric_range(amount_eur, 1.0, 50000.0)


def validate_headline(text: str) -> bool:
    """RSA headline: 1-30 characters."""
    return validate_string_length(text.strip(), 1, 30)


def validate_description(text: str) -> bool:
    """RSA description: 1-90 characters."""
    return validate_string_length(text.strip(), 1, 90)


def validate_match_type(match_type: str) -> bool:
    """BROAD, PHRASE, EXACT."""
    return validate_enum(match_type, ["BROAD", "PHRASE", "EXACT"])


def validate_bid_amount(amount_eur: float) -> bool:
    """Bid: >= 0.01 EUR, <= 100.00 EUR."""
    return validate_numeric_range(amount_eur, 0.01, 100.0)


def validate_keyword_text(text: str) -> bool:
    """Keyword: 1-80 characters, not empty after strip."""
    return validate_string_length(text.strip(), 1, 80)


def validate_url(url: str) -> bool:
    """Basic URL validation."""
    return bool(re.match(r"^https?://\S+", url))


def validate_mode(mode: str) -> bool:
    """Mode must be 'preview' or 'execute'."""
    return validate_enum(mode, ["preview", "execute"])


def euros_to_micros(euros: float) -> int:
    """Convert: 1.50 EUR → 1500000 micros."""
    return int(round(euros * 1_000_000))


def micros_to_euros(micros: int) -> float:
    """Convert: 1500000 micros → 1.50 EUR."""
    return micros / 1_000_000
