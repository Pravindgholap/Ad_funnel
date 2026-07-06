"""
Mock Meta Ads API.

This mock intentionally replicates THREE real-world API pain points,
because a pipeline that only handles the happy path isn't production-grade:

1. PAGINATION  -> /campaigns returns paged results with a `next_cursor`,
                  exactly like Meta's Graph API `paging.cursors.after`.
2. RATE LIMITING -> Every 5th request returns 429, simulating Meta's
                  actual rate-limit behavior under load.
3. TRANSIENT FAILURES -> ~10% of requests to /insights return 500,
                  simulating flaky upstream infra.

Your extraction client (pipeline/extract.py) must survive all three.
"""
import random
from fastapi import FastAPI, HTTPException, Query
from datetime import datetime, timezone
from mock_api.fake_data import ALL_CAMPAIGNS, ALL_INSIGHTS

app = FastAPI(title="Mock Meta Ads API", version="0.2.0")

PAGE_SIZE = 10
_request_counter = {"count": 0}  # simple in-memory counter for rate-limit simulation


@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "service": "mock-meta-ads-api",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _maybe_rate_limit():
    """Every 5th request across the whole server gets rate-limited."""
    _request_counter["count"] += 1
    if _request_counter["count"] % 5 == 0:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Retry after backoff.",
        )


@app.get("/campaigns")
def get_campaigns(cursor: int = Query(0, ge=0)):
    """
    Paginated campaign listing — mirrors Meta Graph API's cursor pattern.

    Why cursor-based, not page-number-based:
    Real ad platforms (Meta, Google) use cursor pagination because
    page-number pagination breaks when records are added/removed
    mid-pagination. Building your client against cursors now means
    the skill transfers directly to the real Meta API later.
    """
    _maybe_rate_limit()

    start = cursor
    end = cursor + PAGE_SIZE
    page_data = ALL_CAMPAIGNS[start:end]

    next_cursor = end if end < len(ALL_CAMPAIGNS) else None

    return {
        "data": page_data,
        "paging": {
            "cursors": {"after": next_cursor},
            "next": next_cursor is not None,
        },
    }


@app.get("/insights/{campaign_id}")
def get_insights(campaign_id: str):
    """
    Returns daily performance rows for a campaign.
    ~10% chance of a simulated transient 500 error, to force
    retry logic to actually prove itself.
    """
    _maybe_rate_limit()

    if random.random() < 0.10:
        raise HTTPException(
            status_code=500,
            detail="Internal upstream error (simulated transient failure).",
        )

    if campaign_id not in ALL_INSIGHTS:
        raise HTTPException(status_code=404, detail=f"Unknown campaign_id: {campaign_id}")

    return {"data": ALL_INSIGHTS[campaign_id]}