"""
Extraction client: pulls campaigns + insights from the mock Meta Ads API.

This is the file that most directly answers the JD's:
"Experience with data ingestion from APIs (basic handling of
pagination, retries)."

Design decisions explained inline.
"""
import json
import logging
from datetime import datetime, timezone

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from config.settings import MOCK_API_BASE_URL, MOCK_API_TIMEOUT_SECONDS, RAW_DATA_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("adfunnel.extract")


class TransientAPIError(Exception):
    """
    Raised for errors we consider WORTH RETRYING (429, 500, timeouts).
    We deliberately do NOT retry on 404 — that's a permanent error
    (the resource doesn't exist), and retrying it just wastes time
    and hides a real bug. This distinction — retryable vs. permanent
    errors — is the single most important design decision in any
    retry strategy.
    """
    pass


@retry(
    retry=retry_if_exception_type(TransientAPIError),
    stop=stop_after_attempt(4),                     # max 4 attempts total
    wait=wait_exponential(multiplier=1, min=1, max=10),  # 1s, 2s, 4s, 8s(capped 10s)
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,  # after exhausting retries, raise the real exception (don't swallow)
)
def _get(url: str, params: dict = None) -> dict:
    """
    Single low-level HTTP GET wrapped in retry logic.

    Why isolate this in its own function instead of decorating the
    whole extraction pipeline:
    Retries should wrap the SMALLEST possible unit of work (one HTTP
    call), not a whole multi-step function. Otherwise you risk
    re-running side effects (like writing partial files) on retry.
    """
    try:
        response = requests.get(url, params=params, timeout=MOCK_API_TIMEOUT_SECONDS)
    except requests.exceptions.Timeout as e:
        raise TransientAPIError(f"Timeout calling {url}") from e
    except requests.exceptions.ConnectionError as e:
        raise TransientAPIError(f"Connection error calling {url}") from e

    if response.status_code == 429:
        raise TransientAPIError(f"Rate limited (429) on {url}")
    if response.status_code >= 500:
        raise TransientAPIError(f"Server error ({response.status_code}) on {url}")
    if response.status_code == 404:
        # Permanent error — do NOT retry, fail loud and immediately.
        response.raise_for_status()

    response.raise_for_status()  # catches any other unexpected 4xx
    return response.json()


def extract_all_campaigns() -> list[dict]:
    """
    Pulls ALL campaigns by following the cursor pagination trail.

    Why a while-loop over cursor, not a fixed range:
    We don't know upfront how many pages exist — exactly like the
    real Meta Graph API. Hardcoding "fetch 5 pages" is a rookie
    mistake that silently drops data the moment campaign count grows.
    """
    all_campaigns = []
    cursor = 0
    page_num = 1

    while True:
        logger.info(f"Fetching campaigns page {page_num} (cursor={cursor})")
        payload = _get(f"{MOCK_API_BASE_URL}/campaigns", params={"cursor": cursor})

        all_campaigns.extend(payload["data"])

        next_cursor = payload["paging"]["cursors"]["after"]
        if next_cursor is None:
            break  # no more pages

        cursor = next_cursor
        page_num += 1

    logger.info(f"Extracted {len(all_campaigns)} total campaigns across {page_num} pages")
    return all_campaigns


def extract_insights_for_campaigns(campaign_ids: list[str]) -> list[dict]:
    """
    Pulls insights for each campaign individually.

    Why not fail the whole batch if ONE campaign's insights call fails:
    In a real pipeline, one bad campaign_id shouldn't take down your
    entire ingestion run. We isolate failures per-campaign and log
    them, so 46 successful campaigns still load even if 1 fails.
    This is a data engineering maturity signal: partial failure
    handling, not all-or-nothing brittleness.
    """
    all_insights = []
    failed_campaigns = []

    for cid in campaign_ids:
        try:
            payload = _get(f"{MOCK_API_BASE_URL}/insights/{cid}")
            all_insights.extend(payload["data"])
        except Exception as e:
            logger.error(f"Failed to extract insights for {cid} after retries: {e}")
            failed_campaigns.append(cid)

    if failed_campaigns:
        logger.warning(
            f"{len(failed_campaigns)}/{len(campaign_ids)} campaigns failed extraction: "
            f"{failed_campaigns}"
        )

    return all_insights


def save_raw(data: list[dict], filename: str) -> str:
    """
    Lands raw extracted data to disk as JSON before any transformation.

    Why land raw data at all instead of piping straight to transform:
    This is the 'E' in ETL as a durable checkpoint. If transform.py
    crashes, we don't need to re-hit the API (which costs rate-limit
    budget in real life) — we just re-run transform against the raw
    file already on disk. This is the same principle behind
    Airflow's practice of keeping intermediate task outputs.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filepath = RAW_DATA_DIR / f"{filename}_{timestamp}.json"

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Saved {len(data)} records to {filepath}")
    return str(filepath)


def run_extraction():
    """Entry point: orchestrates the full extraction stage."""
    logger.info("=== Starting extraction ===")

    campaigns = extract_all_campaigns()
    campaigns_path = save_raw(campaigns, "campaigns")

    campaign_ids = [c["campaign_id"] for c in campaigns]
    insights = extract_insights_for_campaigns(campaign_ids)
    insights_path = save_raw(insights, "insights")

    logger.info("=== Extraction complete ===")
    return {"campaigns_path": campaigns_path, "insights_path": insights_path}


if __name__ == "__main__":
    run_extraction()