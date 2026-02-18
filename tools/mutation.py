"""Preview/Execute confirm pattern for all write tools.

Every mutation follows:
1. mode="preview" → read current state, show diff, ask confirmation
2. mode="execute" → apply mutation, log audit, return result
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class MutationPreview:
    """Result of preview phase (mode="preview")."""

    tool_name: str
    client_name: str
    customer_id: str
    action: str
    changes: list  # [{"field": "Budget", "old": "€50", "new": "€75"}]
    warnings: list = field(default_factory=list)
    estimated_impact: Optional[str] = None


@dataclass
class MutationResult:
    """Result of execute phase (mode="execute")."""

    success: bool
    resource_name: Optional[str] = None
    resource_id: Optional[str] = None
    message: Optional[str] = None
    error: Optional[str] = None
    request_id: Optional[str] = None


def format_preview_for_llm(preview: MutationPreview) -> str:
    """Format preview as markdown for LLM confirmation."""
    lines = [f"## Preview: {preview.action}"]
    lines.append(f"**Client**: {preview.client_name} ({preview.customer_id})")
    lines.append("")
    lines.append("| Field | Current | New |")
    lines.append("|-------|---------|-----|")
    for change in preview.changes:
        lines.append(
            f"| {change['field']} | {change.get('old', '—')} | {change['new']} |"
        )
    if preview.warnings:
        lines.append("")
        for w in preview.warnings:
            lines.append(f"⚠️ {w}")
    if preview.estimated_impact:
        lines.append(f"\n📊 {preview.estimated_impact}")
    lines.append("\n**Call again with mode='execute' to apply.**")
    return "\n".join(lines)


def format_result_for_llm(result: MutationResult) -> str:
    """Format execution result as markdown."""
    if result.success:
        return f"✅ {result.message}"
    return f"❌ Error: {result.error}"
