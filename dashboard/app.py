import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# ---------- Page config ----------
st.set_page_config(
    page_title="Retail Medallion Pipeline Dashboard",
    page_icon="📦",
    layout="wide",
)

# ---------- Data loading ----------
DATA_DIR = Path(__file__).parent.parent / "data"


@st.cache_data
def load_data():
    revenue = pd.read_csv(DATA_DIR / "gold_monthly_revenue.csv")
    status = pd.read_csv(DATA_DIR / "gold_order_status.csv")
    health = pd.read_csv(DATA_DIR / "gold_pipeline_health.csv")
    return revenue, status, health


try:
    revenue_df, status_df, health_df = load_data()
except FileNotFoundError as e:
    st.error(
        f"Could not find expected CSV files in `data/`. "
        f"Make sure gold_monthly_revenue.csv, gold_order_status.csv, "
        f"and gold_pipeline_health.csv are exported there.\n\n{e}"
    )
    st.stop()

# ---------- Header ----------
st.title("📦 Retail Medallion Pipeline")
st.caption(
    "Bronze → Silver → Gold data pipeline built on Databricks Free Edition, "
    "orchestrated as an automated job with data quality checks and Unity Catalog lineage tracking."
)

st.markdown(
    "[![GitHub](https://img.shields.io/badge/GitHub-Repo-black?logo=github)]"
    "(https://github.com/hardy07/retail-medallion-pipeline/)"
)

st.divider()

# ---------- Top-level KPIs ----------
total_revenue = revenue_df["total_revenue"].sum()
total_orders = revenue_df["order_count"].sum()
avg_order_value = revenue_df["avg_order_value"].mean()

bronze_count = int(health_df.loc[health_df["layer"] == "bronze", "record_count"].sum())
silver_valid_count = int(health_df.loc[health_df["layer"] == "silver_valid", "record_count"].sum())
quarantine_count = int(health_df.loc[health_df["layer"] == "silver_quarantined", "record_count"].sum())
quality_pass_rate = (
    silver_valid_count / (silver_valid_count + quarantine_count) * 100
    if (silver_valid_count + quarantine_count) > 0
    else 100
)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Revenue", f"${total_revenue:,.0f}")
col2.metric("Total Orders", f"{total_orders:,}")
col3.metric("Avg Order Value", f"${avg_order_value:,.2f}")
col4.metric("Data Quality Pass Rate", f"{quality_pass_rate:.1f}%")

st.divider()

# ---------- Revenue trend ----------
st.subheader("Monthly Revenue Trend")

revenue_df["period"] = (
    revenue_df["order_year"].astype(str) + "-" + revenue_df["order_month"].astype(str).str.zfill(2)
)

fig_revenue = px.line(
    revenue_df,
    x="period",
    y="total_revenue",
    markers=True,
    labels={"period": "Month", "total_revenue": "Revenue ($)"},
)
fig_revenue.update_layout(hovermode="x unified")
st.plotly_chart(fig_revenue, use_container_width=True)

# ---------- Order status + Pipeline health side by side ----------
left, right = st.columns(2)

with left:
    st.subheader("Order Status Breakdown")
    fig_status = px.bar(
        status_df.sort_values("order_count", ascending=True),
        x="order_count",
        y="o_orderstatus",
        orientation="h",
        labels={"order_count": "Number of Orders", "o_orderstatus": "Status"},
        color="o_orderstatus",
    )
    fig_status.update_layout(showlegend=False)
    st.plotly_chart(fig_status, use_container_width=True)

with right:
    st.subheader("Pipeline Data Quality")
    fig_health = go.Figure(
        data=[
            go.Bar(
                x=health_df["layer"],
                y=health_df["record_count"],
                marker_color=["#4C78A8", "#54A24B", "#E45756"],
                text=health_df["record_count"],
                textposition="auto",
            )
        ]
    )
    fig_health.update_layout(
        xaxis_title="Pipeline Layer",
        yaxis_title="Record Count",
    )
    st.plotly_chart(fig_health, use_container_width=True)
    st.caption(
        f"{quarantine_count:,} records failed validation and were routed to "
        f"the quarantine table instead of being silently dropped."
    )

st.divider()

# ---------- Architecture note ----------
with st.expander("ℹ️ About this pipeline"):
    st.markdown(
        """
        This dashboard reads from the **Gold layer** of a Medallion Architecture pipeline
        (Bronze → Silver → Gold) built and orchestrated on **Databricks Free Edition**.

        - **Bronze**: raw ingestion, tagged with source + ingestion timestamp
        - **Silver**: deduplicated, validated, standardized — invalid records are
          quarantined rather than dropped
        - **Gold**: business-facing aggregates powering this dashboard

        Orchestrated as a scheduled **Databricks Job** with task-level dependencies,
        and tracked end-to-end via **Unity Catalog lineage**.

        See the full write-up, architecture diagram, and pipeline screenshots on
        [GitHub](https://github.com/<your-username>/retail-medallion-pipeline).
        """
    )

st.caption("Built by Sakthivel — Data Engineer")