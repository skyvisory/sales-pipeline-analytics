# ============================================
# app.py
# Sales Pipeline Analytics Dashboard
# Built with Streamlit + Plotly + DuckDB
# ============================================

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys
import tempfile

# ============================================
# Page config
# ============================================

st.set_page_config(
    page_title="Sales Pipeline Analytics",
    page_icon="📊",
    layout="wide"
)

# ============================================
# Paths
# ============================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IS_CLOUD = os.path.exists('/mount/src')
DATA_DIR = tempfile.gettempdir() if IS_CLOUD else os.path.join(BASE_DIR, 'data')
CSV_PATH = os.path.join(DATA_DIR, 'opportunities.csv')

# ============================================
# Generate data if needed
# ============================================

sys.path.append(os.path.join(BASE_DIR, 'scripts'))
from startup import generate_csv

if not os.path.exists(CSV_PATH):
    generate_csv(DATA_DIR)

# ============================================
# In-memory DuckDB — works on cloud and local
# ============================================

df_raw = pd.read_csv(CSV_PATH)
conn   = duckdb.connect()
conn.register('opportunities', df_raw)

# ============================================
# Global font size
# ============================================

FONT_SIZE = 14

# ============================================
# Database connection
# ============================================

import os
import sys
import tempfile
import pandas as pd

BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IS_CLOUD  = os.path.exists('/mount/src')
DATA_DIR  = tempfile.gettempdir() if IS_CLOUD else os.path.join(BASE_DIR, 'data')
CSV_PATH  = os.path.join(DATA_DIR, 'opportunities.csv')

sys.path.append(os.path.join(BASE_DIR, 'scripts'))
from startup import generate_csv

if not os.path.exists(CSV_PATH):
    generate_csv(DATA_DIR)

# Load CSV into memory — no DuckDB needed on cloud
import duckdb
conn = duckdb.connect()  # in-memory database
df_raw = pd.read_csv(CSV_PATH)
conn.register('opportunities', df_raw)

# ============================================
# Data loading
# ============================================

@st.cache_data
def load_pipeline_overview():
    return conn.execute("""
        SELECT
            stage,
            COUNT(*)                                AS opportunities,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*))
                OVER (), 1)                         AS pct_of_total,
            ROUND(AVG(acv), 0)                      AS avg_acv,
            SUM(acv)                                AS total_acv
        FROM opportunities
        GROUP BY stage
        ORDER BY CASE stage
            WHEN 'Prospecting' THEN 1
            WHEN 'Qualified'   THEN 2
            WHEN 'Proposal'    THEN 3
            WHEN 'Negotiation' THEN 4
            WHEN 'Closed Won'  THEN 5
            WHEN 'Closed Lost' THEN 6
        END
    """).fetchdf()

@st.cache_data
def load_rep_leaderboard():
    return conn.execute("""
        SELECT
            rep_name,
            region,
            COUNT(*)                                        AS total_deals,
            SUM(CASE WHEN stage = 'Closed Won'
                THEN 1 ELSE 0 END)                         AS deals_won,
            ROUND(SUM(CASE WHEN stage = 'Closed Won'
                THEN 1.0 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN stage
                IN ('Closed Won', 'Closed Lost')
                THEN 1 ELSE 0 END), 0) * 100, 1)           AS win_rate_pct,
            SUM(CASE WHEN stage = 'Closed Won'
                THEN acv ELSE 0 END)                       AS total_revenue,
            quota,
            ROUND(SUM(CASE WHEN stage = 'Closed Won'
                THEN acv ELSE 0 END)
                * 100.0 / quota, 1)                        AS quota_attainment_pct
        FROM opportunities
        GROUP BY rep_name, region, quota
        ORDER BY quota_attainment_pct ASC
    """).fetchdf()

@st.cache_data
def load_quarterly_revenue():
    return conn.execute("""
        SELECT
            'Q' || CAST(CEIL(CAST(EXTRACT(MONTH FROM close_date::DATE)
                AS DOUBLE) / 3) AS INTEGER)
                || ' ' ||
                CAST(EXTRACT(YEAR FROM close_date::DATE)
                AS INTEGER)                                 AS close_quarter,
            COUNT(CASE WHEN stage = 'Closed Won'
                THEN 1 END)                                 AS deals_won,
            SUM(CASE WHEN stage = 'Closed Won'
                THEN acv ELSE 0 END)                        AS revenue_recognised,
            ROUND(AVG(CASE WHEN stage = 'Closed Won'
                THEN acv END), 0)                           AS avg_deal_size
        FROM opportunities
        WHERE stage = 'Closed Won'
        GROUP BY close_quarter
        ORDER BY MIN(close_date::DATE)
    """).fetchdf()

@st.cache_data
def load_pipeline_health():
    return conn.execute("""
        WITH pipeline_metrics AS (
            SELECT
                rep_name,
                region,
                quota,
                ROUND(SUM(CASE WHEN stage = 'Closed Won'
                    THEN 1.0 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN stage
                    IN ('Closed Won', 'Closed Lost')
                    THEN 1 ELSE 0 END), 0), 3)              AS close_rate,
                ROUND(SUM(CASE WHEN stage NOT IN
                    ('Closed Won', 'Closed Lost')
                    THEN acv ELSE 0 END)
                    * 1.0 / quota, 3)                       AS actual_coverage,
                ROUND(SUM(CASE WHEN stage = 'Closed Won'
                    THEN 1.0 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN stage
                    IN ('Closed Won', 'Closed Lost')
                    THEN 1 ELSE 0 END), 0), 3)              AS win_rate,
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
                ROUND(1.0 / NULLIF(close_rate, 0), 2)      AS required_coverage,
                ROUND(LEAST(actual_coverage
                    / NULLIF(1.0 / NULLIF(close_rate, 0),
                    0), 1.0) * 40, 1)                       AS coverage_score,
                ROUND(win_rate * 35, 1)                     AS win_rate_score,
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
        ORDER BY pipeline_health_score ASC
    """).fetchdf()

@st.cache_data
def load_summary_metrics():
    return conn.execute("""
        SELECT
            COUNT(*)                                        AS total_opportunities,
            SUM(CASE WHEN stage = 'Closed Won'
                THEN acv ELSE 0 END)                        AS total_revenue,
            ROUND(SUM(CASE WHEN stage = 'Closed Won'
                THEN 1.0 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN stage
                IN ('Closed Won', 'Closed Lost')
                THEN 1 ELSE 0 END), 0) * 100, 1)           AS overall_win_rate,
            SUM(CASE WHEN stage NOT IN
                ('Closed Won', 'Closed Lost')
                THEN acv ELSE 0 END)                        AS active_pipeline,
            ROUND(AVG(CASE WHEN stage
                IN ('Closed Won', 'Closed Lost')
                THEN days_in_pipeline END), 0)              AS avg_cycle_days,
            COUNT(DISTINCT rep_name)                        AS total_reps
        FROM opportunities
    """).fetchdf()

# ============================================
# Load all data
# ============================================

df_overview  = load_pipeline_overview()
df_reps      = load_rep_leaderboard()
df_quarterly = load_quarterly_revenue()
df_health    = load_pipeline_health()
df_summary   = load_summary_metrics()

# ============================================
# Header
# ============================================

st.title("📊 Sales Pipeline Analytics")
st.caption("Fintech SaaS — 2023-2024 | Built with DuckDB + Streamlit")

st.divider()

# ============================================
# Row 1 — KPI cards
# ============================================

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        label="Total Revenue",
        value=f"${df_summary['total_revenue'].iloc[0]/1e6:.1f}M"
    )
with col2:
    st.metric(
        label="Active Pipeline",
        value=f"${df_summary['active_pipeline'].iloc[0]/1e6:.1f}M"
    )
with col3:
    st.metric(
        label="Overall Win Rate",
        value=f"{df_summary['overall_win_rate'].iloc[0]}%"
    )
with col4:
    st.metric(
        label="Avg Sales Cycle",
        value=f"{df_summary['avg_cycle_days'].iloc[0]:.0f} days"
    )
with col5:
    st.metric(
        label="Total Opportunities",
        value=f"{df_summary['total_opportunities'].iloc[0]:,}"
    )

st.divider()

# ============================================
# Row 2 — Pipeline Funnel + Quarterly Revenue
# ============================================

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Pipeline Funnel")

    funnel_stages = ['Qualified', 'Proposal', 'Negotiation', 'Closed Won']
    df_funnel = df_overview[
        df_overview['stage'].isin(funnel_stages)
    ].copy()

    fig_funnel = go.Figure(go.Funnel(
        y=df_funnel['stage'],
        x=df_funnel['opportunities'],
        textinfo="value+percent initial",
        textfont=dict(size=FONT_SIZE),
        marker=dict(color=[
            '#3498db',
            '#e67e22',
            '#e74c3c',
            '#2ecc71'
        ])
    ))
    fig_funnel.update_layout(
        height=400,
        margin=dict(l=0, r=0, t=20, b=0),
        font=dict(size=FONT_SIZE)
    )
    st.plotly_chart(fig_funnel, width='stretch')

with col_right:
    st.subheader("Quarterly Revenue")

    # Format revenue as $XM for display
    df_quarterly['revenue_label'] = df_quarterly[
        'revenue_recognised'
    ].apply(lambda x: f"${x/1e6:.1f}M")

    fig_quarterly = go.Figure(go.Bar(
        x=df_quarterly['close_quarter'],
        y=df_quarterly['revenue_recognised'],
        text=df_quarterly['revenue_label'],
        textposition='outside',
        marker_color='#3498db'
    ))
    fig_quarterly.update_layout(
        height=400,
        xaxis_title='Quarter',
        yaxis_title='Revenue',
        margin=dict(l=0, r=0, t=20, b=0),
        font=dict(size=FONT_SIZE),
        yaxis=dict(
            tickformat='$,.0f',
            tickprefix=''
        )
    )
    st.plotly_chart(fig_quarterly, width='stretch')

st.divider()

# ============================================
# Row 3 — Rep Leaderboard + Pipeline Health
# ============================================

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Rep Leaderboard — Quota Attainment")

    colors = [
        '#2ecc71' if x >= 100 else
        '#e67e22' if x >= 80 else
        '#e74c3c'
        for x in df_reps['quota_attainment_pct']
    ]

    fig_reps = go.Figure(go.Bar(
        x=df_reps['quota_attainment_pct'],
        y=df_reps['rep_name'],
        orientation='h',
        marker_color=colors,
        text=[f"{x:.1f}%" for x in df_reps['quota_attainment_pct']],
        textposition='outside',
        textfont=dict(size=FONT_SIZE)
    ))
    fig_reps.update_layout(
        height=400,
        xaxis_title='Quota Attainment (%)',
        yaxis_title='',
        margin=dict(l=0, r=80, t=20, b=0),
        xaxis=dict(range=[0, 140]),
        font=dict(size=FONT_SIZE)
    )
    fig_reps.add_vline(
        x=100,
        line_dash='dash',
        line_color='gray',
        annotation_text='Quota',
        annotation_position='top',
        annotation_font_size=FONT_SIZE
    )
    st.plotly_chart(fig_reps, width='stretch')

with col_right:
    st.subheader("Pipeline Health Score")

    health_colors = {
        'Strong':   '#2ecc71',
        'Healthy':  '#3498db',
        'At Risk':  '#e67e22',
        'Critical': '#e74c3c'
    }

    fig_health = go.Figure(go.Bar(
        x=df_health['pipeline_health_score'],
        y=df_health['rep_name'],
        orientation='h',
        marker_color=[
            health_colors[s] for s in df_health['health_status']
        ],
        text=[
            f"{s} ({v:.1f})"
            for s, v in zip(
                df_health['health_status'],
                df_health['pipeline_health_score']
            )
        ],
        textposition='outside',
        textfont=dict(size=FONT_SIZE)
    ))
    fig_health.update_layout(
        height=400,
        xaxis_title='Pipeline Health Score (0-100)',
        yaxis_title='',
        margin=dict(l=0, r=140, t=20, b=0),
        xaxis=dict(range=[0, 105]),
        font=dict(size=FONT_SIZE)
    )
    fig_health.add_vline(
        x=80, line_dash='dash',
        line_color='#2ecc71',
        annotation_text='Strong',
        annotation_position='top',
        annotation_font_size=FONT_SIZE
    )
    fig_health.add_vline(
        x=60, line_dash='dash',
        line_color='#e67e22',
        annotation_text='At Risk',
        annotation_position='top',
        annotation_font_size=FONT_SIZE
    )
    st.plotly_chart(fig_health, width='stretch')

st.divider()

# ============================================
# Row 4 — Win Rate by Segment + Deal Source
# ============================================

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Win Rate by Segment")

    df_segment = conn.execute("""
        SELECT
            segment,
            ROUND(SUM(CASE WHEN stage = 'Closed Won'
                THEN 1.0 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN stage
                IN ('Closed Won', 'Closed Lost')
                THEN 1 ELSE 0 END), 0) * 100, 1) AS win_rate_pct,
            ROUND(AVG(CASE WHEN stage = 'Closed Won'
                THEN acv END), 0)                 AS avg_won_acv
        FROM opportunities
        GROUP BY segment
        ORDER BY win_rate_pct DESC
    """).fetchdf()

    fig_segment = go.Figure(go.Bar(
        x=df_segment['segment'],
        y=df_segment['win_rate_pct'],
        text=[f"{x:.1f}%" for x in df_segment['win_rate_pct']],
        textposition='outside',
        textfont=dict(size=FONT_SIZE),
        marker_color='#3498db'
    ))
    fig_segment.update_layout(
        height=350,
        xaxis_title='Segment',
        yaxis_title='Win Rate (%)',
        margin=dict(l=0, r=0, t=20, b=0),
        font=dict(size=FONT_SIZE),
        yaxis=dict(range=[0, 60])
    )
    st.plotly_chart(fig_segment, width='stretch')

with col_right:
    st.subheader("Win Rate by Deal Source")

    df_source = conn.execute("""
        SELECT
            deal_source,
            ROUND(SUM(CASE WHEN stage = 'Closed Won'
                THEN 1.0 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN stage
                IN ('Closed Won', 'Closed Lost')
                THEN 1 ELSE 0 END), 0) * 100, 1) AS win_rate_pct,
            SUM(CASE WHEN stage = 'Closed Won'
                THEN acv ELSE 0 END)              AS total_revenue
        FROM opportunities
        GROUP BY deal_source
        ORDER BY win_rate_pct DESC
    """).fetchdf()

    fig_source = go.Figure(go.Bar(
        x=df_source['deal_source'],
        y=df_source['win_rate_pct'],
        text=[f"{x:.1f}%" for x in df_source['win_rate_pct']],
        textposition='outside',
        textfont=dict(size=FONT_SIZE),
        marker_color='#e67e22'
    ))
    fig_source.update_layout(
        height=350,
        xaxis_title='Deal Source',
        yaxis_title='Win Rate (%)',
        margin=dict(l=0, r=0, t=20, b=0),
        font=dict(size=FONT_SIZE),
        yaxis=dict(range=[0, 60])
    )
    st.plotly_chart(fig_source, width='stretch')

st.divider()

# ============================================
# Row 5 — Pipeline Health Score detail table
# ============================================

st.subheader("Pipeline Health Score — Full Detail")

# Keep numeric copy for conditional formatting
health_score_numeric = df_health['pipeline_health_score'].copy()

# Rename columns for readability
df_display = df_health[[
    'rep_name', 'region',
    'actual_coverage', 'required_coverage',
    'win_rate_pct', 'avg_cycle_days',
    'coverage_score', 'win_rate_score',
    'velocity_score', 'pipeline_health_score',
    'health_status'
]].copy()

df_display.columns = [
    'Rep', 'Region',
    'Actual Coverage', 'Required Coverage',
    'Win Rate', 'Avg Cycle Days',
    'Coverage Score', 'Win Rate Score',
    'Velocity Score', 'Health Score',
    'Status'
]

# Format columns
df_display['Win Rate']           = df_display['Win Rate'].apply(lambda x: f"{x:.1f}%")
df_display['Actual Coverage']    = df_display['Actual Coverage'].apply(lambda x: f"{x:.2f}x")
df_display['Required Coverage']  = df_display['Required Coverage'].apply(lambda x: f"{x:.2f}x")
df_display['Coverage Score']     = df_display['Coverage Score'].apply(lambda x: f"{x:.1f}")
df_display['Win Rate Score']     = df_display['Win Rate Score'].apply(lambda x: f"{x:.1f}")
df_display['Velocity Score']     = df_display['Velocity Score'].apply(lambda x: f"{x:.1f}")
df_display['Health Score']       = df_display['Health Score'].apply(lambda x: f"{x:.1f}")

# Conditional formatting functions
def color_health(val):
    # Uses original numeric values not formatted strings
    pass

def color_status(val):
    colors = {
        'Strong':   'background-color: #27ae60; color: white',
        'Healthy':  'background-color: #2ecc71; color: white',
        'At Risk':  'background-color: #e74c3c; color: white',
        'Critical': 'background-color: #922b21; color: white'
    }
    return colors.get(val, '')

def color_health_col(col):
    styles = []
    for v in health_score_numeric:
        if v > 80:
            styles.append('background-color: #27ae60; color: white')
        elif v > 60:
            styles.append('background-color: #2ecc71; color: white')
        elif v > 40:
            styles.append('background-color: #e74c3c; color: white')
        else:
            styles.append('background-color: #922b21; color: white')
    return styles

styled = df_display.style \
    .map(color_status, subset=['Status']) \
    .apply(color_health_col, subset=['Health Score'])

st.dataframe(
    styled,
    width='stretch',
    hide_index=True
)

st.divider()
st.caption(
    "Data: Synthetic CRM dataset — 2,000 opportunities | "
    "Built with DuckDB + Streamlit + Plotly"
)