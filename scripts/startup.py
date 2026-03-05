# ============================================
# startup.py
# Purpose: Generate and load data if database doesn't exist — runs on app startup
# ============================================

import os
import duckdb
import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

def generate_and_load(data_dir='data', db_path=None):
    """Generate synthetic data and load into DuckDB"""

    if db_path is None:
        db_path = os.path.join(data_dir, 'pipeline.duckdb')

    print(f"Generating synthetic data to {db_path}...")

    fake = Faker()
    Faker.seed(42)
    np.random.seed(42)
    random.seed(42)

    N_OPPORTUNITIES = 2000
    START_DATE = datetime(2023, 1, 1)
    END_DATE   = datetime(2024, 12, 31)

    REPS = [
        {'name': 'Sarah Chen',    'region': 'APAC',  'quota': 6000000},
        {'name': 'Marcus Webb',   'region': 'APAC',  'quota': 6500000},
        {'name': 'Priya Sharma',  'region': 'EMEA',  'quota': 7000000},
        {'name': 'James Okafor',  'region': 'EMEA',  'quota': 7500000},
        {'name': 'Lisa Park',     'region': 'AMER',  'quota': 8000000},
        {'name': 'Tom Reilly',    'region': 'AMER',  'quota': 9000000},
        {'name': 'Ana Souza',     'region': 'LATAM', 'quota': 5000000},
        {'name': 'David Kim',     'region': 'LATAM', 'quota': 6000000},
    ]

    STAGES = [
        'Prospecting', 'Qualified', 'Proposal',
        'Negotiation', 'Closed Won', 'Closed Lost'
    ]

    STAGE_DAYS = {
        'Prospecting': (7,  21),
        'Qualified':   (14, 30),
        'Proposal':    (14, 45),
        'Negotiation': (7,  30),
        'Closed Won':  (0,  0),
        'Closed Lost': (0,  0)
    }

    INDUSTRIES   = ['Banking', 'Insurance', 'Payments',
                    'Lending', 'Wealth Management', 'Crypto']
    DEAL_SOURCES = ['Inbound', 'Outbound', 'Referral', 'Partner', 'Event']

    ACV_SEGMENTS = {
        'SMB':        (10000,  50000,  0.50),
        'Mid-Market': (50000,  200000, 0.35),
        'Enterprise': (200000, 500000, 0.15)
    }

    def random_date(start, end):
        delta = end - start
        return start + timedelta(days=random.randint(0, delta.days))

    def generate_acv():
        segment = random.choices(
            list(ACV_SEGMENTS.keys()),
            weights=[v[2] for v in ACV_SEGMENTS.values()]
        )[0]
        min_acv, max_acv, _ = ACV_SEGMENTS[segment]
        acv = round(random.randint(min_acv, max_acv) / 5000) * 5000
        return acv, segment

    def determine_stage(acv):
        win_prob = 0.20 if acv > 200000 else 0.25 if acv > 50000 else 0.32
        rand = random.random()
        if rand < win_prob:              return 'Closed Won'
        elif rand < win_prob + 0.40:     return 'Closed Lost'
        elif rand < win_prob + 0.55:     return 'Negotiation'
        elif rand < win_prob + 0.70:     return 'Proposal'
        elif rand < win_prob + 0.82:     return 'Qualified'
        else:                            return 'Prospecting'

    def calc_days(stage):
        total = 0
        for s in STAGES[:STAGES.index(stage) + 1]:
            if STAGE_DAYS[s][1] > 0:
                total += random.randint(STAGE_DAYS[s][0], STAGE_DAYS[s][1])
        return total

    opportunities = []
    for i in range(N_OPPORTUNITIES):
        rep          = random.choice(REPS)
        acv, segment = generate_acv()
        stage        = determine_stage(acv)
        created_date = random_date(START_DATE, END_DATE)
        days         = calc_days(stage)
        close_date   = min(created_date + timedelta(days=days), END_DATE)

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
            'days_in_pipeline': days,
            'quarter':          f"Q{(created_date.month-1)//3+1} "
                               f"{created_date.year}",
        })

    df = pd.DataFrame(opportunities)

    # Use passed data_dir and db_path — not hardcoded
    os.makedirs(data_dir, exist_ok=True)
    df.to_csv(os.path.join(data_dir, 'opportunities.csv'), index=False)

    conn = duckdb.connect(db_path)
    conn.execute("DROP TABLE IF EXISTS opportunities")
    conn.register('df_temp', df)
    conn.execute("CREATE TABLE opportunities AS SELECT * FROM df_temp")
    conn.unregister('df_temp')
    conn.execute("""
        CREATE OR REPLACE VIEW closed_deals AS
        SELECT * FROM opportunities
        WHERE stage IN ('Closed Won', 'Closed Lost')
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW active_pipeline AS
        SELECT * FROM opportunities
        WHERE stage NOT IN ('Closed Won', 'Closed Lost')
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW won_deals AS
        SELECT * FROM opportunities
        WHERE stage = 'Closed Won'
    """)
    conn.close()
    print(f"Data generated and loaded to {db_path}")

if __name__ == '__main__':
    generate_and_load()