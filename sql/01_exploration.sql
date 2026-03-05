-- ============================================
-- 01_exploration.sql
-- Sales Pipeline Analysis
-- 10 business queries covering pipeline health, rep performance, and revenue analytics
-- ============================================

-- ============================================
-- Query 1: Pipeline Overview
-- What is the overall health of our pipeline?
-- ============================================

SELECT
    stage,
    COUNT(*)                                    AS opportunities,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*))
        OVER (), 1)                             AS pct_of_total,
    ROUND(AVG(acv), 0)                          AS avg_acv,
    SUM(acv)                                    AS total_acv,
    ROUND(AVG(days_in_pipeline), 0)             AS avg_days_in_pipeline
FROM opportunities
GROUP BY stage
ORDER BY
    CASE stage
        WHEN 'Prospecting' THEN 1
        WHEN 'Qualified'   THEN 2
        WHEN 'Proposal'    THEN 3
        WHEN 'Negotiation' THEN 4
        WHEN 'Closed Won'  THEN 5
        WHEN 'Closed Lost' THEN 6
    END;

-- ============================================
-- Query 2: Win Rate Analysis
-- What is our overall and segmented win rate?
-- ============================================

SELECT
    segment,
    COUNT(*)                                            AS total_deals,
    SUM(CASE WHEN stage = 'Closed Won' THEN 1 ELSE 0
        END)                                            AS won,
    SUM(CASE WHEN stage = 'Closed Lost' THEN 1 ELSE 0
        END)                                            AS lost,
    ROUND(SUM(CASE WHEN stage = 'Closed Won'
        THEN 1.0 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN stage
        IN ('Closed Won', 'Closed Lost')
        THEN 1 ELSE 0 END), 0) * 100, 1)               AS win_rate_pct,
    ROUND(AVG(CASE WHEN stage = 'Closed Won'
        THEN acv END), 0)                               AS avg_won_acv
FROM opportunities
GROUP BY segment
ORDER BY win_rate_pct DESC;

-- ============================================
-- Query 3: Rep Leaderboard
-- Who are the top performers?
-- ============================================

SELECT
    rep_name,
    region,
    COUNT(*)                                            AS total_deals,
    SUM(CASE WHEN stage = 'Closed Won' THEN 1 ELSE 0
        END)                                            AS deals_won,
    ROUND(SUM(CASE WHEN stage = 'Closed Won'
        THEN 1.0 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN stage
        IN ('Closed Won', 'Closed Lost')
        THEN 1 ELSE 0 END), 0) * 100, 1)               AS win_rate_pct,
    SUM(CASE WHEN stage = 'Closed Won'
        THEN acv ELSE 0 END)                            AS total_revenue,
    quota,
    ROUND(SUM(CASE WHEN stage = 'Closed Won'
        THEN acv ELSE 0 END)
        * 100.0 / quota, 1)                             AS quota_attainment_pct
FROM opportunities
GROUP BY rep_name, region, quota
ORDER BY quota_attainment_pct DESC;

-- ============================================
-- Query 4: Average Sales Cycle
-- How long does it take to close a deal?
-- ============================================

SELECT
    segment,
    ROUND(AVG(CASE WHEN stage = 'Closed Won'
        THEN days_in_pipeline END), 0)                  AS avg_days_to_win,
    ROUND(AVG(CASE WHEN stage = 'Closed Lost'
        THEN days_in_pipeline END), 0)                  AS avg_days_to_lose,
    ROUND(AVG(CASE WHEN stage = 'Closed Won'
        THEN acv END), 0)                               AS avg_won_acv
FROM opportunities
GROUP BY segment
ORDER BY avg_days_to_win DESC;

-- ============================================
-- Query 5: Pipeline Coverage Ratio
-- Do we have enough pipeline to hit quota?
-- Coverage ratio = pipeline value / quota
-- Healthy = 3x or above
-- Works backwards from actual close rate
-- Required coverage = 1 / close_rate
-- ============================================

WITH close_rates AS (
    SELECT
        rep_name,
        quota,
        region,
        -- Actual close rate from historical data
        ROUND(SUM(CASE WHEN stage = 'Closed Won'
            THEN 1.0 ELSE 0 END)
            / NULLIF(SUM(CASE WHEN stage
            IN ('Closed Won', 'Closed Lost')
            THEN 1 ELSE 0 END), 0), 3)          AS close_rate,
        -- Active pipeline value
        SUM(CASE WHEN stage NOT IN
            ('Closed Won', 'Closed Lost')
            THEN acv ELSE 0 END)                AS active_pipeline,
        -- Revenue already closed
        SUM(CASE WHEN stage = 'Closed Won'
            THEN acv ELSE 0 END)                AS revenue_closed
    FROM opportunities
    GROUP BY rep_name, quota, region
),
coverage AS (
    SELECT
        rep_name,
        region,
        quota,
        close_rate,
        active_pipeline,
        revenue_closed,
        -- Required coverage derived from close rate
        ROUND(1.0 / NULLIF(close_rate, 0), 2)  AS required_coverage,
        -- Actual coverage
        ROUND(active_pipeline * 1.0
            / quota, 2)                         AS actual_coverage
    FROM close_rates
)
SELECT
    rep_name,
    region,
    quota,
    ROUND(close_rate * 100, 1)                  AS close_rate_pct,
    active_pipeline,
    revenue_closed,
    required_coverage,
    actual_coverage,
    ROUND(actual_coverage
        - required_coverage, 2)                 AS coverage_gap,
    CASE
        WHEN actual_coverage
            >= required_coverage THEN 'Sufficient'
        WHEN actual_coverage
            >= required_coverage * 0.75 THEN 'At Risk'
        ELSE 'Insufficient'
    END                                         AS coverage_status
FROM coverage
ORDER BY coverage_gap ASC;

-- ============================================
-- Query 6: Quarterly Revenue
-- How does revenue trend across quarters?
-- Uses CLOSED date for revenue recognition
-- Uses CREATED date for pipeline creation
-- ============================================

SELECT
    'Q' || CAST(CEIL(CAST(EXTRACT(MONTH FROM close_date::DATE)
        AS DOUBLE) / 3) AS INTEGER)
        || ' ' ||
        CAST(EXTRACT(YEAR FROM close_date::DATE) AS INTEGER) AS close_quarter,
    COUNT(CASE WHEN stage = 'Closed Won'
        THEN 1 END)                             AS deals_won,
    SUM(CASE WHEN stage = 'Closed Won'
        THEN acv ELSE 0 END)                    AS revenue_recognised,
    ROUND(AVG(CASE WHEN stage = 'Closed Won'
        THEN acv END), 0)                       AS avg_deal_size,
    COUNT(CASE WHEN stage NOT IN
        ('Closed Won', 'Closed Lost')
        THEN 1 END)                             AS active_deals_created,
    SUM(CASE WHEN stage NOT IN
        ('Closed Won', 'Closed Lost')
        THEN acv ELSE 0 END)                    AS pipeline_created
FROM opportunities
WHERE stage = 'Closed Won'
   OR stage NOT IN ('Closed Won', 'Closed Lost')
GROUP BY close_quarter
ORDER BY MIN(close_date::DATE);

-- ============================================
-- Query 7: Win Rate by Deal Source
-- Which lead sources produce the best deals?
-- ============================================

SELECT
    deal_source,
    COUNT(*)                                            AS total_deals,
    ROUND(SUM(CASE WHEN stage = 'Closed Won'
        THEN 1.0 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN stage
        IN ('Closed Won', 'Closed Lost')
        THEN 1 ELSE 0 END), 0) * 100, 1)               AS win_rate_pct,
    ROUND(AVG(CASE WHEN stage = 'Closed Won'
        THEN acv END), 0)                               AS avg_won_acv,
    SUM(CASE WHEN stage = 'Closed Won'
        THEN acv ELSE 0 END)                            AS total_revenue
FROM opportunities
GROUP BY deal_source
ORDER BY win_rate_pct DESC;

-- ============================================
-- Query 8: Stage Velocity
-- Where do deals stall in the pipeline?
-- ============================================

SELECT
    stage,
    COUNT(*)                                            AS deals,
    ROUND(AVG(days_in_pipeline), 0)                     AS avg_days,
    MIN(days_in_pipeline)                               AS min_days,
    MAX(days_in_pipeline)                               AS max_days,
    ROUND(AVG(acv), 0)                                  AS avg_acv
FROM opportunities
WHERE stage NOT IN ('Closed Won', 'Closed Lost')
GROUP BY stage
ORDER BY
    CASE stage
        WHEN 'Prospecting' THEN 1
        WHEN 'Qualified'   THEN 2
        WHEN 'Proposal'    THEN 3
        WHEN 'Negotiation' THEN 4
    END;

-- ============================================
-- Query 9: Regional Performance
-- How do regions compare on key metrics?
-- ============================================

SELECT
    region,
    COUNT(*)                                            AS total_deals,
    ROUND(SUM(CASE WHEN stage = 'Closed Won'
        THEN 1.0 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN stage
        IN ('Closed Won', 'Closed Lost')
        THEN 1 ELSE 0 END), 0) * 100, 1)                AS win_rate_pct,
    SUM(CASE WHEN stage = 'Closed Won'
        THEN acv ELSE 0 END)                            AS total_revenue,
    ROUND(AVG(CASE WHEN stage = 'Closed Won'
        THEN acv END), 0)                               AS avg_won_acv,
    AVG(DISTINCT quota)                                 AS avg_quota
FROM opportunities
GROUP BY region
ORDER BY total_revenue DESC;

-- ============================================
-- Query 10: Pipeline Health Score
-- Composite metric combining coverage ratio, win rate and velocity into a single score
--
-- Formula:
-- Coverage Score  = MIN(coverage_ratio / 3, 1) × 40
-- Win Rate Score  = win_rate × 35
-- Velocity Score  = MIN(1, 90 / avg_days) × 25
--
-- Total: 0-100
-- > 80  → Strong
-- 60-80 → Healthy
-- 40-60 → At Risk
-- < 40  → Critical
-- ============================================

WITH pipeline_metrics AS (
    SELECT
        rep_name,
        region,
        quota,
        -- Close rate
        ROUND(SUM(CASE WHEN stage = 'Closed Won'
            THEN 1.0 ELSE 0 END)
            / NULLIF(SUM(CASE WHEN stage
            IN ('Closed Won', 'Closed Lost')
            THEN 1 ELSE 0 END), 0), 3)              AS close_rate,
        -- Active pipeline
        ROUND(SUM(CASE WHEN stage NOT IN
            ('Closed Won', 'Closed Lost')
            THEN acv ELSE 0 END)
            * 1.0 / quota, 3)                       AS actual_coverage,
        -- Win rate
        ROUND(SUM(CASE WHEN stage = 'Closed Won'
            THEN 1.0 ELSE 0 END)
            / NULLIF(SUM(CASE WHEN stage
            IN ('Closed Won', 'Closed Lost')
            THEN 1 ELSE 0 END), 0), 3)              AS win_rate,
        -- Avg cycle days
        ROUND(AVG(CASE WHEN stage
            IN ('Closed Won', 'Closed Lost')
            THEN days_in_pipeline END), 0)          AS avg_cycle_days
    FROM opportunities
    GROUP BY rep_name, region, quota
),
scores AS (
    SELECT
        rep_name,
        region,
        quota,
        close_rate,
        actual_coverage,
        win_rate,
        avg_cycle_days,
        -- Required coverage from close rate
        ROUND(1.0 / NULLIF(close_rate, 0), 2)      AS required_coverage,
        -- Coverage score: actual vs required
        ROUND(LEAST(actual_coverage
            / NULLIF(1.0 / NULLIF(close_rate, 0),
            0), 1.0) * 40, 1)                       AS coverage_score,
        -- Win rate score
        ROUND(win_rate * 35, 1)                     AS win_rate_score,
        -- Velocity score
        ROUND(LEAST(1.0, 90.0
            / NULLIF(avg_cycle_days, 0)) * 25, 1)   AS velocity_score
    FROM pipeline_metrics
)
SELECT
    rep_name,
    region,
    ROUND(actual_coverage, 2)                       AS actual_coverage,
    ROUND(required_coverage, 2)                     AS required_coverage,
    ROUND(win_rate * 100, 1)                        AS win_rate_pct,
    avg_cycle_days,
    coverage_score,
    win_rate_score,
    velocity_score,
    ROUND(coverage_score
        + win_rate_score
        + velocity_score, 1)                        AS pipeline_health_score,
    CASE
        WHEN coverage_score
           + win_rate_score
           + velocity_score > 80  THEN 'Strong'
        WHEN coverage_score
           + win_rate_score
           + velocity_score > 60  THEN 'Healthy'
        WHEN coverage_score
           + win_rate_score
           + velocity_score > 40  THEN 'At Risk'
        ELSE 'Critical'
    END                                             AS health_status
FROM scores
ORDER BY pipeline_health_score DESC;