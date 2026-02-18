"""Centralized error handling for all tools.

Uniform pattern: Google Ads errors → readable messages for the LLM.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class ErrorCategory(Enum):
    """Error classification."""

    VALIDATION = "VALIDATION"
    API_ERROR = "API_ERROR"
    AUTH_ERROR = "AUTH_ERROR"
    QUOTA_ERROR = "QUOTA_ERROR"
    NOT_FOUND = "NOT_FOUND"
    INTERNAL = "INTERNAL"


@dataclass
class ErrorResult:
    """Structured error for LLM consumption."""

    category: ErrorCategory
    message: str
    details: Optional[dict] = None
    request_id: Optional[str] = None


def handle_google_ads_error(ex) -> ErrorResult:
    """Extract structured errors from GoogleAdsException.

    Returns ErrorResult with readable message, error details, request_id.
    """
    errors = []
    try:
        for error in ex.failure.errors:
            errors.append({
                "message": error.message,
                "code": str(error.error_code),
                "location": str(error.location) if error.location else None,
            })
    except AttributeError:
        errors = [{"message": str(ex), "code": "UNKNOWN"}]

    return ErrorResult(
        category=ErrorCategory.API_ERROR,
        message=f"Google Ads API error: {errors[0]['message']}" if errors else str(ex),
        details={"google_ads_errors": errors},
        request_id=getattr(ex, "request_id", None),
    )


def handle_validation_error(message: str, field_name: str = None) -> ErrorResult:
    """Input validation error."""
    return ErrorResult(
        category=ErrorCategory.VALIDATION,
        message=message,
        details={"field": field_name} if field_name else None,
    )


def handle_not_found_error(entity: str, identifier: str) -> ErrorResult:
    """Entity not found error."""
    return ErrorResult(
        category=ErrorCategory.NOT_FOUND,
        message=f"{entity} '{identifier}' not found.",
    )


def handle_quota_error(remaining: int, required: int) -> ErrorResult:
    """Quota exceeded error."""
    return ErrorResult(
        category=ErrorCategory.QUOTA_ERROR,
        message=f"Quota insufficient: {remaining} ops remaining, {required} required.",
        details={"remaining": remaining, "required": required},
    )


def format_error_for_llm(error: ErrorResult) -> str:
    """Format error as markdown for the LLM."""
    return f"❌ **{error.category.value}**: {error.message}"
