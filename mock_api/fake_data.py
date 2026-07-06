"""
Generates deterministic, realistic-looking Meta Ads data.

Why deterministic (seeded) randomness matters:
If your mock data changes every server restart, you can't debug
reproducibly. A senior engineer's mock always uses a fixed seed
so "it worked yesterday, broke today" bugs are never caused by
the test fixture itself.
"""
import random
from datetime import datetime, timedelta

random.seed(42)  # fixed seed = reproducible test data across runs

CAMPAIGN_NAMES = [
    "FA_PersonalLoans_Q3", "FA_CreditCards_Prospecting",
    "FA_Insurance_Retargeting", "FA_Investing_TopFunnel",
    "FA_Mortgage_Leads", "FA_SavingsAccounts_Awareness",
]

OBJECTIVES = ["LEAD_GENERATION", "CONVERSIONS", "TRAFFIC", "BRAND_AWARENESS"]
STATUSES = ["ACTIVE", "PAUSED", "ARCHIVED"]


def generate_campaigns(n=47):
    """
    Generates n fake campaigns.
    n=47 is deliberately NOT a round number — this forces us to
    actually implement pagination correctly rather than accidentally
    getting lucky with page_size divisibility (e.g. 50/25=2 pages
    exactly, which would hide off-by-one bugs).
    """
    campaigns = []
    for i in range(1, n + 1):
        campaigns.append({
            "campaign_id": f"cmp_{1000 + i}",
            "campaign_name": f"{random.choice(CAMPAIGN_NAMES)}_{i}",
            "objective": random.choice(OBJECTIVES),
            "status": random.choice(STATUSES),
            "daily_budget": round(random.uniform(50, 500), 2),
            "created_time": (
                datetime(2026, 1, 1) + timedelta(days=random.randint(0, 180))
            ).isoformat(),
        })
    return campaigns


def generate_insights_for_campaign(campaign_id, days=30):
    """
    Generates daily performance rows (impressions, clicks, spend, leads)
    for one campaign — this is the raw material for CTR/CPC/CPA in Sprint 2.
    """
    rows = []
    base_date = datetime(2026, 6, 1)
    for d in range(days):
        impressions = random.randint(500, 20000)
        # CTR realistically hovers 0.5%-3% for finance vertical ads
        clicks = int(impressions * random.uniform(0.005, 0.03))
        spend = round(clicks * random.uniform(0.8, 3.5), 2)  # CPC-driven spend
        # Lead conversion rate from clicks, realistically 2%-8%
        leads = int(clicks * random.uniform(0.02, 0.08))
        rows.append({
            "campaign_id": campaign_id,
            "date": (base_date + timedelta(days=d)).strftime("%Y-%m-%d"),
            "impressions": impressions,
            "clicks": clicks,
            "spend": spend,
            "leads": leads,
        })
    return rows


# Pre-generate once at module load (simulates a stable backend dataset)
ALL_CAMPAIGNS = generate_campaigns(47)
ALL_INSIGHTS = {
    c["campaign_id"]: generate_insights_for_campaign(c["campaign_id"])
    for c in ALL_CAMPAIGNS
}