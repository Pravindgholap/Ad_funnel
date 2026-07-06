"""
Pydantic models defining the CONTRACT for what a valid raw record
looks like — enforced at extraction time, before data ever touches
the warehouse.

Why validate here (in Python) AND later in SQL (quality_checks.sql):
These operate at different altitudes. Pydantic validates STRUCTURE
and TYPE correctness of individual records (e.g. "is spend a
non-negative number"). SQL validates SET-LEVEL properties across
the whole table (e.g. "are there duplicate campaign_id+date rows",
"is today's data actually present"). You need both — a single
record can be individually valid but the batch can still be broken
(e.g. missing an entire day).
"""
from datetime import date as date_type
from pydantic import BaseModel, Field, field_validator


class CampaignRecord(BaseModel):
    """Contract for one campaign record from the mock API."""
    campaign_id: str
    campaign_name: str
    objective: str
    status: str
    daily_budget: float = Field(ge=0)  # budget can never be negative
    created_time: str

    @field_validator("objective")
    @classmethod
    def objective_must_be_known(cls, v: str) -> str:
        """
        Why validate against a known set of objectives:
        If Meta (or our mock) ever introduces a new objective type
        we don't recognize, we want to KNOW about it explicitly
        rather than silently ingesting a value our downstream SQL
        and dashboard filters have never seen and don't handle.
        """
        allowed = {"LEAD_GENERATION", "CONVERSIONS", "TRAFFIC", "BRAND_AWARENESS"}
        if v not in allowed:
            raise ValueError(f"Unknown objective '{v}', expected one of {allowed}")
        return v

    @field_validator("status")
    @classmethod
    def status_must_be_known(cls, v: str) -> str:
        allowed = {"ACTIVE", "PAUSED", "ARCHIVED"}
        if v not in allowed:
            raise ValueError(f"Unknown status '{v}', expected one of {allowed}")
        return v


class InsightRecord(BaseModel):
    """
    Contract for one daily insight row.

    Why the funnel-logic validator matters more than the type checks:
    Type checks (impressions is an int) catch garbage. The
    funnel-logic check (clicks <= impressions) catches data that is
    STRUCTURALLY valid but LOGICALLY impossible — you cannot click
    an ad more times than it was shown. This is the kind of check
    that only someone who understands the marketing funnel (not just
    generic data validation) would think to write, which is exactly
    the "marketing funnel understanding" line in the JD.
    """
    campaign_id: str
    date: date_type
    impressions: int = Field(ge=0)
    clicks: int = Field(ge=0)
    spend: float = Field(ge=0)
    leads: int = Field(ge=0)

    @field_validator("clicks")
    @classmethod
    def clicks_cannot_exceed_impressions(cls, v: int, info) -> int:
        impressions = info.data.get("impressions")
        if impressions is not None and v > impressions:
            raise ValueError(
                f"Funnel violation: clicks ({v}) > impressions ({impressions})"
            )
        return v

    @field_validator("leads")
    @classmethod
    def leads_cannot_exceed_clicks(cls, v: int, info) -> int:
        clicks = info.data.get("clicks")
        if clicks is not None and v > clicks:
            raise ValueError(
                f"Funnel violation: leads ({v}) > clicks ({clicks})"
            )
        return v