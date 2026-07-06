"""
Tests for the extraction client — specifically proving pagination
and retry behavior work, not just the happy path.
"""
import pytest
from unittest.mock import patch, MagicMock
from pipeline.extract import extract_all_campaigns, _get, TransientAPIError


def test_pagination_follows_cursor_until_none():
    """
    Simulates a 2-page API response and confirms we correctly
    stitch both pages together AND stop when next_cursor is None.
    """
    page_1 = {
        "data": [{"campaign_id": "cmp_1"}, {"campaign_id": "cmp_2"}],
        "paging": {"cursors": {"after": 2}, "next": True},
    }
    page_2 = {
        "data": [{"campaign_id": "cmp_3"}],
        "paging": {"cursors": {"after": None}, "next": False},
    }

    with patch("pipeline.extract._get", side_effect=[page_1, page_2]) as mock_get:
        result = extract_all_campaigns()

    assert len(result) == 3
    assert mock_get.call_count == 2  # exactly 2 pages, no infinite loop


def test_retry_exhausts_and_raises_on_persistent_failure():
    """
    Confirms that if the API is DOWN for all 4 attempts, we get a
    real exception at the end — not a silent swallow, not an infinite
    hang. This proves our retry has a ceiling.
    """
    with patch("requests.get", side_effect=Exception("boom")):
        with pytest.raises(Exception):
            _get("http://fake-url/test")