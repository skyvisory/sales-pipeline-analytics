import duckdb
import pandas as pd
import re

# ============================================
# run_queries.py
# Purpose: Execute all queries from sql/01_exploration.sql against DuckDB and print results
# References SQL file directly — no hardcoded queries in this script
# ============================================

conn = duckdb.connect('data/pipeline.duckdb')

# --- Read SQL file ---
with open('sql/01_exploration.sql', 'r') as f:
    sql_content = f.read()

# --- Split into individual queries ---
# Split on the header comment pattern
queries = re.split(r'--\s*={3,}.*?Query \d+.*?={3,}.*?\n', 
                   sql_content, 
                   flags=re.DOTALL)

# Remove empty splits
queries = [q.strip() for q in queries if q.strip()]

# Extract query titles from comments
titles = re.findall(r'--\s*Query \d+[:\-]?\s*(.*?)\n', sql_content)

print(f"Found {len(queries)} queries in sql/01_exploration.sql\n")

# --- Execute each query ---
for i, (query, title) in enumerate(zip(queries, titles), 1):
    # Remove remaining comment lines
    clean_query = '\n'.join(
        line for line in query.split('\n')
        if not line.strip().startswith('--')
    ).strip()

    # Skip empty queries
    if not clean_query:
        continue

    print(f"\n{'=' * 55}")
    print(f"Query {i}: {title.strip()}")
    print('=' * 55)

    try:
        result = conn.execute(clean_query).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")

conn.close()
print("\n=== All queries complete ===")
