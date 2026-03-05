# ============================================
# load_database.py
# Purpose: Load generated CSV into DuckDB
#          Create views for SQL analysis
# ============================================

import duckdb
import pandas as pd
import os

# ============================================
# Connect to DuckDB — file based, no server needed
# Creates the file if it doesn't exist
# ============================================

os.makedirs('data', exist_ok=True)
conn = duckdb.connect('data/pipeline.duckdb')

# ============================================
# Load CSV into DuckDB table
# ============================================

df = pd.read_csv('data/opportunities.csv')

# Drop existing table if rerunning
conn.execute("DROP TABLE IF EXISTS opportunities")

# Create table from DataFrame
conn.execute("""
    CREATE TABLE opportunities AS
    SELECT * FROM df
""")

# Verify
result = conn.execute("""
    SELECT COUNT(*) as total_rows,
           COUNT(DISTINCT opportunity_id) as unique_opps,
           COUNT(DISTINCT rep_name) as reps,
           MIN(created_date) as earliest,
           MAX(created_date) as latest
    FROM opportunities
""").fetchdf()

print("=== DATABASE LOADED ===")
print(result.to_string(index=False))

# ============================================
# Create useful views for SQL analysis
# ============================================

# View 1 — Closed deals only
conn.execute("""
    CREATE OR REPLACE VIEW closed_deals AS
    SELECT *
    FROM opportunities
    WHERE stage IN ('Closed Won', 'Closed Lost')
""")

# View 2 — Active pipeline only
conn.execute("""
    CREATE OR REPLACE VIEW active_pipeline AS
    SELECT *
    FROM opportunities
    WHERE stage NOT IN ('Closed Won', 'Closed Lost')
""")

# View 3 — Won deals only
conn.execute("""
    CREATE OR REPLACE VIEW won_deals AS
    SELECT *
    FROM opportunities
    WHERE stage = 'Closed Won'
""")

print("\n=== VIEWS CREATED ===")
print("  · closed_deals")
print("  · active_pipeline")
print("  · won_deals")

# ============================================
# Quick data profile
# ============================================

profile = conn.execute("""
    SELECT
        stage,
        COUNT(*) as opportunities,
        ROUND(AVG(acv), 0) as avg_acv,
        SUM(acv) as total_acv,
        ROUND(AVG(days_in_pipeline), 0) as avg_days
    FROM opportunities
    GROUP BY stage
    ORDER BY avg_acv DESC
""").fetchdf()

print("\n=== DATA PROFILE BY STAGE ===")
print(profile.to_string(index=False))

conn.close()
print("\nDatabase saved to data/pipeline.duckdb")