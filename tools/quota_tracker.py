"""API quota tracking — monitors daily operation consumption.

Standard Access MCC: every GAQL query = 1 op, every mutate with N operations = N ops.
"""

import os
import logging
from datetime import date, datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_quota_tracker = None


class QuotaTracker:
    """Tracks API operations consumed per day."""

    def __init__(self, supabase_url: str, supabase_key: str, daily_limit: int = 15000):
        from supabase import create_client

        self.client = create_client(supabase_url, supabase_key)
        self.table = "mcp_quota_usage"
        self.daily_limit = daily_limit
        # In-memory cache for current day
        self._cache_date = None
        self._cache_count = 0

    def track(
        self,
        operation_type: str,
        count: int = 1,
        customer_id: str = None,
        tool_name: str = None,
    ):
        """Record operations consumed. Fire-and-forget."""
        try:
            today = date.today().isoformat()
            self.client.table(self.table).insert({
                "date": today,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "operation_type": operation_type,
                "count": count,
                "customer_id": customer_id,
                "tool_name": tool_name,
            }).execute()
            # Update cache
            if self._cache_date == today:
                self._cache_count += count
            else:
                self._cache_date = today
                self._cache_count = count
        except Exception as e:
            logger.error(f"Quota tracking failed: {e}")

    def get_remaining(self) -> int:
        """Get remaining operations for today."""
        try:
            today = date.today().isoformat()
            result = self.client.table(self.table).select("count").eq("date", today).execute()
            used = sum(row["count"] for row in result.data) if result.data else 0
            return max(0, self.daily_limit - used)
        except Exception as e:
            logger.error(f"Quota check failed: {e}")
            return self.daily_limit  # Fail open

    def can_execute(self, estimated_ops: int = 1) -> bool:
        """Check if enough quota remains."""
        return self.get_remaining() >= estimated_ops


def get_quota_tracker() -> Optional[QuotaTracker]:
    """Get or create the singleton quota tracker."""
    global _quota_tracker
    if _quota_tracker is None:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if url and key:
            try:
                _quota_tracker = QuotaTracker(url, key)
            except Exception as e:
                logger.error(f"Failed to init quota tracker: {e}")
    return _quota_tracker
