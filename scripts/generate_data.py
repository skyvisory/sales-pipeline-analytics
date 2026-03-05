# ============================================
# generate_data.py
# Purpose: Generate synthetic CRM dataset simulating 2 years of sales pipeline data for a fintech SaaS company
#
# Generates:
# · 2,000 opportunities
# · 8 sales reps across 4 regions
# · 6 pipeline stages
# · Realistic win rates and sales cycles
# · Saved to data/opportunities.csv
# ============================================

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# ============================================
# Configuration — all business logic here
# No hardcoding elsewhere in the script
# ============================================

N_OPPORTUNITIES = 2000
START_DATE = datetime(2023, 1, 1)
END_DATE   = datetime(2024, 12, 31)

REPS = [
    {'name': 'Sarah Chen',    'region': 'APAC',   'quota': 6000000},
    {'name': 'Marcus Webb',   'region': 'APAC',   'quota': 6500000},
    {'name': 'Priya Sharma',  'region': 'EMEA',   'quota': 7000000},
    {'name': 'James Okafor',  'region': 'EMEA',   'quota': 7500000},
    {'name': 'Lisa Park',     'region': 'AMER',   'quota': 8000000},
    {'name': 'Tom Reilly',    'region': 'AMER',   'quota': 9000000},
    {'name': 'Ana Souza',     'region': 'LATAM',  'quota': 5000000},
    {'name': 'David Kim',     'region': 'LATAM',  'quota': 6000000},
]

STAGES = [
    'Prospecting',
    'Qualified',
    'Proposal',
    'Negotiation',
    'Closed Won',
    'Closed Lost'
]

# Probability of reaching each stage from previous
STAGE_CONVERSION = {
    'Prospecting': 1.00,
    'Qualified':   0.65,
    'Proposal':    0.50,
    'Negotiation': 0.35,
    'Closed Won':  0.25,
    'Closed Lost': 0.75   # of those that don't close won
}

# Average days spent in each stage
STAGE_DAYS = {
    'Prospecting': (7,  21),   # min, max days
    'Qualified':   (14, 30),
    'Proposal':    (14, 45),
    'Negotiation': (7,  30),
    'Closed Won':  (0,  0),
    'Closed Lost': (0,  0)
}

INDUSTRIES = [
    'Banking',
    'Insurance',
    'Payments',
    'Lending',
    'Wealth Management',
    'Crypto'
]

DEAL_SOURCES = [
    'Inbound',
    'Outbound',
    'Referral',
    'Partner',
    'Event'
]

# ACV ranges by deal size segment
ACV_SEGMENTS = {
    'SMB':        (10000,  50000,  0.50),  # min, max, probability
    'Mid-Market': (50000,  200000, 0.35),
    'Enterprise': (200000, 500000, 0.15)
}

# ============================================
# Helper functions
# ============================================

def random_date(start, end):
    delta = end - start
    days  = random.randint(0, delta.days)
    return start + timedelta(days=days)

def generate_acv():
    segment = random.choices(
        list(ACV_SEGMENTS.keys()),
        weights=[v[2] for v in ACV_SEGMENTS.values()]
    )[0]
    min_acv, max_acv, _ = ACV_SEGMENTS[segment]
    acv = random.randint(min_acv, max_acv)
    # Round to nearest 5,000
    acv = round(acv / 5000) * 5000
    return acv, segment

def determine_stage(acv):
    # Enterprise deals less likely to close
    if acv > 200000:
        win_prob = 0.20
    elif acv > 50000:
        win_prob = 0.25
    else:
        win_prob = 0.32

    rand = random.random()
    if rand < win_prob:
        return 'Closed Won'
    elif rand < win_prob + 0.40:
        return 'Closed Lost'
    elif rand < win_prob + 0.55:
        return 'Negotiation'
    elif rand < win_prob + 0.70:
        return 'Proposal'
    elif rand < win_prob + 0.82:
        return 'Qualified'
    else:
        return 'Prospecting'

def calculate_days_in_pipeline(stage, created_date):
    total_days = 0
    stages_passed = STAGES[:STAGES.index(stage) + 1]
    for s in stages_passed:
        if STAGE_DAYS[s][1] > 0:
            total_days += random.randint(
                STAGE_DAYS[s][0],
                STAGE_DAYS[s][1]
            )
    return total_days

# ============================================
# Generate opportunities
# ============================================

opportunities = []

for i in range(N_OPPORTUNITIES):
    rep         = random.choice(REPS)
    acv, segment = generate_acv()
    stage       = determine_stage(acv)
    created_date = random_date(START_DATE, END_DATE)
    days_in_pipeline = calculate_days_in_pipeline(stage, created_date)
    close_date  = created_date + timedelta(days=days_in_pipeline)

    # Cap close date at end of dataset period
    if close_date > END_DATE:
        close_date = END_DATE

    opportunities.append({
        'opportunity_id':   f'OPP-{i+1:04d}',
        'company_name':     fake.company(),
        'rep_name':         rep['name'],
        'region':           rep['region'],
        'quota':            rep['quota'],
        'stage':            stage,
        'acv':              acv,
        'segment':          segment,
        'industry':         random.choice(INDUSTRIES),
        'deal_source':      random.choice(DEAL_SOURCES),
        'created_date':     created_date.strftime('%Y-%m-%d'),
        'close_date':       close_date.strftime('%Y-%m-%d'),
        'days_in_pipeline': days_in_pipeline,
        'quarter':          f"Q{(created_date.month - 1) // 3 + 1} "
                           f"{created_date.year}",
    })

# ============================================
# Save to CSV
# ============================================

df = pd.DataFrame(opportunities)

os.makedirs('data', exist_ok=True)
df.to_csv('data/opportunities.csv', index=False)

print("=== SYNTHETIC DATASET GENERATED ===")
print(f"Total opportunities: {len(df):,}")
print(f"\nStage distribution:")
print(df['stage'].value_counts())
print(f"\nWin rate: {(df['stage'] == 'Closed Won').mean()*100:.1f}%")
print(f"Average ACV: ${df['acv'].mean():,.0f}")
print(f"Total pipeline value: ${df['acv'].sum():,.0f}")
print(f"\nRep distribution:")
print(df['rep_name'].value_counts())
print(f"\nSaved to data/opportunities.csv")