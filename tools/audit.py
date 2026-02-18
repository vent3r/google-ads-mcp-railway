"""Audit logger — logs every mutation to Supabase.

Table: mcp_audit_log (create via SQL in Fase 0C docs).
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Lazy-loaded Supabase client
_audit_logger = None


class AuditLogger:
    """Logs mutations to Supabase mcp_audit_log table."""

    def __init__(self, supabase_url: str, supabase_key: str):
        from supabase import create_client

        self.client = create_client(supabase_url, supabase_key)
        self.table = "mcp_audit_log"

    def log_mutation(
        self,
        customer_id: str,
        client_name: str,
        tool_name: str,
        action: str,
        parameters: dict,
        old_values: dict,
        new_values: dict,
        success: bool,
        error_message: Optional[str] = None,
        request_id: Optional[str] = None,
    ):
        """Log a mutation to Supabase. Fire-and-forget — never blocks tool execution."""
        try:
            self.client.table(self.table).insert({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "customer_id": customer_id,
                "client_name": client_name,
                "tool_name": tool_name,
                "action": action,
                "parameters": parameters,
                "old_values": old_values,
                "new_values": new_values,
                "success": success,
                "error_message": error_message,
                "request_id": request_id,
            }).execute()
        except Exception as e:
            logger.error(f"Audit log failed: {e}")


def get_audit_logger() -> Optional[AuditLogger]:
    """Get or create the singleton audit logger."""
    global _audit_logger
    if _audit_logger is None:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if url and key:
            try:
                _audit_logger = AuditLogger(url, key)
            except Exception as e:
                logger.error(f"Failed to init audit logger: {e}")
    return _audit_logger
