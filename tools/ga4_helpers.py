"""Shared infrastructure for GA4 (Google Analytics Data API) tools.

- get_credentials: service account JSON (GA4_SERVICE_ACCOUNT_JSON) or ADC
- PropertyResolver: GA4 property name/ID mapping via Admin API (24h cache)
- run_ga4_report: Data API runReport wrapper returning list of row dicts
- ga4_date_range: 'last_7_days' / 'YYYY-MM-DD,YYYY-MM-DD' → (start, end)

Auth priority:
  1. GA4_SERVICE_ACCOUNT_JSON  — full JSON of a service account key.
     The SA email must be added as Viewer on each GA4 property.
  2. GA4_SERVICE_ACCOUNT_FILE  — path to the same JSON on disk.
  3. Application Default Credentials (the OAuth refresh token used for
     Google Ads). Works only if that token was issued with the
     analytics.readonly scope.
"""

import json
import logging
import os
import threading
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

GA4_SCOPE = "https://www.googleapis.com/auth/analytics.readonly"


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

_creds = None
_creds_lock = threading.Lock()


def get_credentials():
    """Return google.auth credentials with the analytics.readonly scope."""
    global _creds
    with _creds_lock:
        if _creds is not None:
            return _creds

        from google.oauth2 import service_account

        raw = os.environ.get("GA4_SERVICE_ACCOUNT_JSON", "").strip()
        path = os.environ.get("GA4_SERVICE_ACCOUNT_FILE", "").strip()

        if raw:
            info = json.loads(raw)
            _creds = service_account.Credentials.from_service_account_info(
                info, scopes=[GA4_SCOPE]
            )
            logger.info("GA4 auth: service account (env JSON) %s", info.get("client_email"))
        elif path:
            _creds = service_account.Credentials.from_service_account_file(
                path, scopes=[GA4_SCOPE]
            )
            logger.info("GA4 auth: service account (file) %s", path)
        else:
            import google.auth

            _creds, _ = google.auth.default(scopes=[GA4_SCOPE])
            logger.info("GA4 auth: application default credentials")
        return _creds


def _data_client():
    from google.analytics.data_v1beta import BetaAnalyticsDataClient

    return BetaAnalyticsDataClient(credentials=get_credentials())


def _admin_client():
    from google.analytics.admin_v1beta import AnalyticsAdminServiceClient

    return AnalyticsAdminServiceClient(credentials=get_credentials())


def _friendly_error(e: Exception) -> ValueError:
    msg = str(e)
    if "PERMISSION_DENIED" in msg or "403" in msg:
        return ValueError(
            "GA4 permission denied. Add the service account email as Viewer "
            "on the property, or re-issue the OAuth token with the "
            "analytics.readonly scope. " + msg[:200]
        )
    if "insufficient authentication scopes" in msg.lower():
        return ValueError(
            "GA4 token lacks the analytics.readonly scope. Set "
            "GA4_SERVICE_ACCOUNT_JSON or re-issue the refresh token. " + msg[:200]
        )
    if "NOT_FOUND" in msg or "404" in msg:
        return ValueError("GA4 property not found. Check the property ID. " + msg[:200])
    return ValueError("GA4 API error: " + msg[:300])


# ---------------------------------------------------------------------------
# Property resolver
# ---------------------------------------------------------------------------

class PropertyResolver:
    """Maps GA4 property names ↔ numeric IDs (24h cache)."""

    _by_name: Dict[str, str] = {}
    _by_id: Dict[str, Dict[str, str]] = {}
    _lock = threading.Lock()
    _last_refresh: Optional[datetime] = None
    _REFRESH_INTERVAL = timedelta(hours=24)

    @classmethod
    def refresh(cls) -> None:
        try:
            client = _admin_client()
            summaries = client.list_account_summaries()
        except Exception as e:
            logger.exception("PropertyResolver.refresh failed")
            raise _friendly_error(e)

        with cls._lock:
            cls._by_name.clear()
            cls._by_id.clear()
            for acct in summaries:
                acct_name = acct.display_name
                acct_id = acct.account.split("/")[-1]
                for prop in acct.property_summaries:
                    pid = prop.property.split("/")[-1]
                    cls._by_name[prop.display_name.lower()] = pid
                    cls._by_id[pid] = {
                        "id": pid,
                        "name": prop.display_name,
                        "account": acct_name,
                        "account_id": acct_id,
                        "type": str(prop.property_type).replace("PropertyType.", ""),
                    }
            cls._last_refresh = datetime.now()
            logger.info("PropertyResolver: %d properties loaded", len(cls._by_id))

    @classmethod
    def ensure_loaded(cls) -> None:
        if cls._last_refresh is None or datetime.now() - cls._last_refresh > cls._REFRESH_INTERVAL:
            cls.refresh()

    @classmethod
    def resolve(cls, prop: str) -> str:
        clean = prop.strip().replace("properties/", "")
        if clean.isdigit():
            return clean
        cls.ensure_loaded()
        with cls._lock:
            pid = cls._by_name.get(clean.lower())
            if pid:
                return pid
            for name, pid in cls._by_name.items():
                if clean.lower() in name:
                    return pid
            available = ", ".join(f"{v['name']} ({k})" for k, v in cls._by_id.items())
        raise ValueError(f"GA4 property '{prop}' not found. Available: {available}")

    @classmethod
    def resolve_name(cls, pid: str) -> str:
        clean = pid.replace("properties/", "")
        try:
            cls.ensure_loaded()
        except Exception:
            return clean
        with cls._lock:
            return cls._by_id.get(clean, {}).get("name", clean)

    @classmethod
    def get_all(cls) -> List[Dict[str, str]]:
        cls.ensure_loaded()
        with cls._lock:
            return sorted(cls._by_id.values(), key=lambda p: (p["account"].lower(), p["name"].lower()))


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

_PRESETS = {
    "today": 0,
    "yesterday": 1,
    "last_7_days": 7,
    "last_14_days": 14,
    "last_28_days": 28,
    "last_30_days": 30,
    "last_90_days": 90,
}


def ga4_date_range(period: str) -> Tuple[str, str]:
    """Return (start_date, end_date) as YYYY-MM-DD.

    Accepts presets (last_7_days, last_30_days, yesterday, today) or an
    explicit 'YYYY-MM-DD,YYYY-MM-DD' pair. Presets end yesterday.
    """
    p = period.strip().lower()
    if "," in p:
        a, b = [s.strip() for s in p.split(",", 1)]
        datetime.strptime(a, "%Y-%m-%d")
        datetime.strptime(b, "%Y-%m-%d")
        return a, b
    if p == "today":
        d = date.today().isoformat()
        return d, d
    if p == "yesterday":
        d = (date.today() - timedelta(days=1)).isoformat()
        return d, d
    days = _PRESETS.get(p)
    if days is None:
        raise ValueError(
            f"Unknown period '{period}'. Use last_7_days, last_30_days, "
            "yesterday, today, or 'YYYY-MM-DD,YYYY-MM-DD'."
        )
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    return start.isoformat(), end.isoformat()


# ---------------------------------------------------------------------------
# Report runner
# ---------------------------------------------------------------------------

def run_ga4_report(
    property_id: str,
    dimensions: List[str],
    metrics: List[str],
    start_date: str,
    end_date: str,
    dimension_filter: Optional[Dict[str, Any]] = None,
    order_by_metric: Optional[str] = None,
    descending: bool = True,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """Run a GA4 Data API report and return rows as dicts.

    dimension_filter: {"field": "eventName", "value": "add_to_cart"} for an
    exact match, or {"field": ..., "in": [..]} for an in-list match.
    Metric values are returned as float where numeric.
    """
    from google.analytics.data_v1beta.types import (
        DateRange,
        Dimension,
        Filter,
        FilterExpression,
        Metric,
        OrderBy,
        RunReportRequest,
    )

    req = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        limit=max(1, min(int(limit), 100000)),
    )

    if dimension_filter:
        field = dimension_filter["field"]
        if "in" in dimension_filter:
            flt = Filter(
                field_name=field,
                in_list_filter=Filter.InListFilter(values=[str(v) for v in dimension_filter["in"]]),
            )
        else:
            flt = Filter(
                field_name=field,
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.EXACT,
                    value=str(dimension_filter["value"]),
                ),
            )
        req.dimension_filter = FilterExpression(filter=flt)

    if order_by_metric:
        req.order_bys = [
            OrderBy(metric=OrderBy.MetricOrderBy(metric_name=order_by_metric), desc=descending)
        ]

    try:
        client = _data_client()
        logger.info(
            "ga4 report prop=%s dims=%s mets=%s %s..%s",
            property_id, dimensions, metrics, start_date, end_date,
        )
        resp = client.run_report(req)
    except Exception as e:
        raise _friendly_error(e)

    dim_names = [h.name for h in resp.dimension_headers]
    met_names = [h.name for h in resp.metric_headers]
    rows: List[Dict[str, Any]] = []
    for r in resp.rows:
        row: Dict[str, Any] = {}
        for name, dv in zip(dim_names, r.dimension_values):
            row[name] = dv.value
        for name, mv in zip(met_names, r.metric_values):
            v = mv.value
            try:
                row[name] = float(v) if "." in v else int(v)
            except (ValueError, TypeError):
                row[name] = v
        rows.append(row)
    return rows


def fmt_num(v: Any) -> str:
    if isinstance(v, float):
        return f"{v:,.2f}"
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)
