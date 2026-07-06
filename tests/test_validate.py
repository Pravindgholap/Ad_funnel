"""
Tests proving validation actually catches bad data — not just that
it runs without error on good data (that's the easy, useless test).
"""
import pytest
from pydantic import ValidationError
from pipeline.schemas import CampaignRecord, InsightRecord


def test_valid_insight_record_passes():
    record = InsightRecord(
        campaign_id="cmp_1", date="2026-06-01",
        impressions=1000, clicks=20, spend=40.0, leads=2,
    )
    assert record.clicks == 20


def test_clicks_exceeding_impressions_is_rejected():
    """
    The funnel-logic test — this is the one that proves you
    understand marketing data, not just generic validation syntax.
    """
    with pytest.raises(ValidationError, match="clicks .* > impressions"):
        InsightRecord(
            campaign_id="cmp_1", date="2026-06-01",
            impressions=100, clicks=500, spend=40.0, leads=2,
        )


def test_leads_exceeding_clicks_is_rejected():
    with pytest.raises(ValidationError, match="leads .* > clicks"):
        InsightRecord(
            campaign_id="cmp_1", date="2026-06-01",
            impressions=1000, clicks=10, spend=40.0, leads=50,
        )


def test_negative_spend_is_rejected():
    with pytest.raises(ValidationError):
        InsightRecord(
            campaign_id="cmp_1", date="2026-06-01",
            impressions=1000, clicks=10, spend=-5.0, leads=1,
        )


def test_unknown_objective_is_rejected():
    with pytest.raises(ValidationError, match="Unknown objective"):
        CampaignRecord(
            campaign_id="cmp_1", campaign_name="Test",
            objective="MOON_LANDING", status="ACTIVE",
            daily_budget=100.0, created_time="2026-01-01T00:00:00",
        )


def test_unknown_status_is_rejected():
    with pytest.raises(ValidationError, match="Unknown status"):
        CampaignRecord(
            campaign_id="cmp_1", campaign_name="Test",
            objective="TRAFFIC", status="ON_FIRE",
            daily_budget=100.0, created_time="2026-01-01T00:00:00",
        )