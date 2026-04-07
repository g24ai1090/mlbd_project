from __future__ import annotations

import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS = PROJECT_ROOT / "results" / "demo" / "benchmark_results.json"
VARIANT_ORDER = ["native", "external", "adaptive"]
VARIANT_COLORS = {
    "native": "#4C6EF5",
    "external": "#D9480F",
    "adaptive": "#0F766E",
}


def load_results(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        html, body, [class*="css"]  {
            font-size: 16px;
        }
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(216, 243, 220, 0.75), transparent 28%),
                radial-gradient(circle at top right, rgba(255, 232, 204, 0.65), transparent 24%),
                linear-gradient(180deg, #f6f7f2 0%, #f3efe5 100%);
        }
        [data-testid="stAppViewContainer"] > .main {
            width: 100%;
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #173f35 0%, #214f44 100%);
            border-right: 1px solid rgba(255,255,255,0.08);
        }
        [data-testid="stSidebar"] * {
            color: #f5f1e8;
        }
        .block-container {
            max-width: min(1500px, 96vw) !important;
            width: 100%;
            padding-top: 2.2rem;
            padding-bottom: 3rem;
            padding-left: 3rem;
            padding-right: 3rem;
        }
        .hero {
            padding: 2rem 2.2rem;
            border-radius: 24px;
            background: linear-gradient(135deg, rgba(255,255,255,0.94), rgba(242, 235, 221, 0.96));
            border: 1px solid rgba(23, 63, 53, 0.08);
            box-shadow: 0 20px 50px rgba(44, 62, 80, 0.10);
            margin-bottom: 1.2rem;
        }
        .hero-kicker {
            font-size: 0.92rem;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            color: #0f766e;
            font-weight: 700;
            margin-bottom: 0.5rem;
        }
        .hero-title {
            font-size: 3.6rem;
            line-height: 0.98;
            font-weight: 800;
            color: #243244;
            margin-bottom: 0.8rem;
            max-width: 980px;
        }
        .hero-copy {
            font-size: 1.12rem;
            color: #4b5563;
            max-width: 900px;
            margin-bottom: 1rem;
        }
        .pill-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.6rem;
            margin-top: 0.2rem;
        }
        .pill {
            padding: 0.45rem 0.8rem;
            border-radius: 999px;
            background: #f0ece3;
            border: 1px solid rgba(36, 50, 68, 0.08);
            color: #243244;
            font-size: 0.94rem;
            font-weight: 600;
        }
        .section-card {
            background: rgba(255,255,255,0.84);
            border: 1px solid rgba(36, 50, 68, 0.08);
            border-radius: 22px;
            padding: 1.35rem 1.35rem 1rem 1.35rem;
            box-shadow: 0 12px 30px rgba(44, 62, 80, 0.06);
        }
        .metric-card {
            border-radius: 20px;
            padding: 1.15rem 1.1rem 1rem 1.1rem;
            min-height: 150px;
            color: #fff;
            box-shadow: 0 12px 28px rgba(44, 62, 80, 0.10);
        }
        .metric-label {
            font-size: 0.92rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            opacity: 0.92;
            margin-bottom: 0.4rem;
        }
        .metric-value {
            font-size: 2.5rem;
            font-weight: 800;
            line-height: 1.05;
            margin-bottom: 0.3rem;
        }
        .metric-detail {
            font-size: 0.98rem;
            opacity: 0.95;
        }
        .metric-native {
            background: linear-gradient(145deg, #3454d1, #4c6ef5);
        }
        .metric-spill {
            background: linear-gradient(145deg, #0f766e, #14b8a6);
        }
        .metric-skew {
            background: linear-gradient(145deg, #8f3f17, #d97706);
        }
        .metric-scale {
            background: linear-gradient(145deg, #4b5563, #111827);
        }
        .callout {
            border-radius: 18px;
            padding: 1rem 1.1rem;
            background: linear-gradient(135deg, rgba(15,118,110,0.12), rgba(76,110,245,0.10));
            border: 1px solid rgba(15,118,110,0.12);
            color: #24414f;
            margin-top: 0.8rem;
        }
        .mini-note {
            color: #5b6470;
            font-size: 1rem;
            margin-bottom: 0.8rem;
        }
        h2, h3 {
            letter-spacing: -0.02em;
        }
        [data-testid="stDataFrame"] {
            font-size: 0.98rem;
        }
        @media (max-width: 1100px) {
            .block-container {
                padding-left: 1.2rem;
                padding-right: 1.2rem;
            }
            .hero {
                padding: 1.4rem;
            }
            .hero-title {
                font-size: 2.5rem;
            }
            .metric-value {
                font-size: 2rem;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def to_benchmark_df(results: dict) -> pd.DataFrame:
    df = pd.DataFrame(results.get("benchmark_results", []))
    if df.empty:
        return df
    df["variant"] = pd.Categorical(df["variant"], categories=VARIANT_ORDER, ordered=True)
    return df.sort_values("variant").reset_index(drop=True)


def render_hero(results: dict) -> None:
    dataset_size = results.get("dataset_size_mb")
    dataset_label = f"{dataset_size:.2f} MB local run" if isinstance(dataset_size, (int, float)) else "Benchmark dataset loaded"
    row_count = f"{results.get('row_count', 0):,} rows"
    st.markdown(
        f"""
        <div class="hero">
            <div class="hero-kicker">Big Data Systems Demo</div>
            <div class="hero-title">Adaptive Distributed Sorting and Analytics Dashboard</div>
            <div class="hero-copy">
                End-to-end benchmark view for the NYC taxi sorting pipeline. This dashboard compares native,
                external, and adaptive strategies, then connects the winner back to business-facing analytics.
            </div>
            <div class="pill-row">
                <div class="pill">{row_count}</div>
                <div class="pill">{dataset_label}</div>
                <div class="pill">Spill-aware benchmark</div>
                <div class="pill">Skew-aware partitioning</div>
                <div class="pill">Analytics-ready outputs</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_metric_cards(results: dict, benchmark_df: pd.DataFrame) -> None:
    summary = results.get("comparison_summary", {})
    adaptive_row = benchmark_df.loc[benchmark_df["variant"] == "adaptive"].iloc[0] if not benchmark_df.empty else None
    best_runtime = (
        benchmark_df.sort_values("execution_seconds").iloc[0]["variant"].title()
        if not benchmark_df.empty
        else "N/A"
    )
    if adaptive_row is not None and "spill_events" in benchmark_df.columns:
        spill_detail = f"Adaptive spill events: {int(adaptive_row['spill_events'])}"
    elif adaptive_row is not None and "disk_bytes_spilled" in benchmark_df.columns:
        spill_detail = f"Adaptive disk spill bytes: {int(adaptive_row['disk_bytes_spilled']):,}"
    else:
        spill_detail = "Adaptive spill metrics unavailable for this run."
    cols = st.columns(4)
    cards = [
        (
            "metric-scale",
            "Rows Processed",
            f"{results.get('row_count', 0):,}",
            f"Best runtime variant: {best_runtime}",
        ),
        (
            "metric-native",
            "Adaptive vs Native",
            f"{summary.get('adaptive_vs_native_runtime_pct', 0)}%",
            "Negative means native is faster on small in-memory runs.",
        ),
        (
            "metric-spill",
            "Spill Reduction",
            f"{summary.get('adaptive_vs_external_spill_reduction_pct', 0)}%",
            spill_detail,
        ),
        (
            "metric-skew",
            "Imbalance Reduction",
            f"{summary.get('adaptive_vs_external_imbalance_reduction_pct', 0)}%",
            f"Adaptive partition ratio: {adaptive_row['partition_imbalance_ratio']:.2f}" if adaptive_row is not None else "",
        ),
    ]
    for col, (card_class, label, value, detail) in zip(cols, cards):
        col.markdown(
            f"""
            <div class="metric-card {card_class}">
                <div class="metric-label">{label}</div>
                <div class="metric-value">{value}</div>
                <div class="metric-detail">{detail}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    if summary.get("demo_note"):
        st.markdown(f'<div class="callout">{summary["demo_note"]}</div>', unsafe_allow_html=True)


def make_variant_chart(df: pd.DataFrame, metric: str, title: str, y_title: str) -> alt.Chart:
    chart_df = df.copy()
    chart_df["variant_label"] = chart_df["variant"].astype(str).str.title()
    return (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusTopLeft=10, cornerRadiusTopRight=10)
        .encode(
            x=alt.X("variant_label:N", title="", sort=[item.title() for item in VARIANT_ORDER]),
            y=alt.Y(f"{metric}:Q", title=y_title),
            color=alt.Color(
                "variant_label:N",
                scale=alt.Scale(
                    domain=[item.title() for item in VARIANT_ORDER],
                    range=[VARIANT_COLORS[item] for item in VARIANT_ORDER],
                ),
                legend=None,
            ),
            tooltip=["variant_label", alt.Tooltip(f"{metric}:Q", format=".4f")],
        )
        .properties(title=title, height=320)
    )


def render_overview_table(benchmark_df: pd.DataFrame) -> None:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Benchmark Matrix")
    st.markdown(
        '<div class="mini-note">This is the result table you can point to during the viva when comparing runtime, spill behavior, and partition skew.</div>',
        unsafe_allow_html=True,
    )
    display_df = benchmark_df.copy()
    if not display_df.empty:
        display_df["variant"] = display_df["variant"].astype(str).str.title()
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


def render_charts(results: dict, benchmark_df: pd.DataFrame) -> None:
    left, right = st.columns(2)
    with left:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.altair_chart(
            make_variant_chart(benchmark_df, "execution_seconds", "Runtime Comparison", "Execution Seconds"),
            use_container_width=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)
    with right:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.altair_chart(
            make_variant_chart(
                benchmark_df,
                "partition_imbalance_ratio",
                "Partition Imbalance",
                "Imbalance Ratio",
            ),
            use_container_width=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)

    analytics = results.get("analytics", {})
    revenue_df = pd.DataFrame(analytics.get("hourly_revenue", []))
    if not revenue_df.empty:
        revenue_df["pickup_hour"] = revenue_df["pickup_hour"].astype(str)
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("Revenue Signal")
        st.markdown(
            '<div class="mini-note">The sorted output immediately feeds downstream analytics. This section shows a sample hourly revenue curve from the processed taxi trips.</div>',
            unsafe_allow_html=True,
        )
        revenue_chart = (
            alt.Chart(revenue_df)
            .mark_area(line={"color": "#8f3f17"}, color="#f4c27a", opacity=0.72)
            .encode(
                x=alt.X("pickup_hour:N", title="Pickup Hour", sort=None),
                y=alt.Y("total_revenue:Q", title="Revenue"),
                tooltip=["pickup_hour", "trips", "total_revenue", "avg_trip_distance"],
            )
            .properties(height=280)
        )
        st.altair_chart(revenue_chart, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)


def render_analytics_tables(results: dict) -> None:
    analytics = results.get("analytics", {})
    hourly_df = pd.DataFrame(analytics.get("hourly_revenue", []))
    pickup_df = pd.DataFrame(analytics.get("top_pickup_zones", []))
    dropoff_df = pd.DataFrame(analytics.get("top_dropoff_zones", []))

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Analytics Outputs")
    hourly_tab, pickup_tab, dropoff_tab = st.tabs(
        ["Hourly Revenue Sample", "Top Pickup Zones", "Top Dropoff Zones"]
    )
    with hourly_tab:
        if hourly_df.empty:
            st.info("This results file does not include hourly analytics.")
        else:
            st.dataframe(hourly_df, use_container_width=True, hide_index=True)
    with pickup_tab:
        if pickup_df.empty:
            st.info("This results file does not include top pickup zones. Re-run the benchmark with the updated pipeline to populate this tab.")
        else:
            st.dataframe(pickup_df, use_container_width=True, hide_index=True)
    with dropoff_tab:
        if dropoff_df.empty:
            st.info("This results file does not include top dropoff zones. Re-run the benchmark with the updated pipeline to populate this tab.")
        else:
            st.dataframe(dropoff_df, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


def main() -> None:
    st.set_page_config(
        page_title="Adaptive Sorting Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_styles()

    path_value = st.sidebar.text_input("Results JSON", value=str(DEFAULT_RESULTS))
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Demo Focus")
    st.sidebar.markdown("- Compare runtime across sorting strategies")
    st.sidebar.markdown("- Show spill and skew improvement from adaptive logic")
    st.sidebar.markdown("- Connect sorted output to analytics results")
    result_path = Path(path_value)
    if not result_path.exists():
        st.error(f"Results file not found: {result_path}")
        return

    results = load_results(result_path)
    benchmark_df = to_benchmark_df(results)

    render_hero(results)
    render_metric_cards(results, benchmark_df)
    st.markdown("<div style='height: 1rem'></div>", unsafe_allow_html=True)
    render_overview_table(benchmark_df)
    st.markdown("<div style='height: 1rem'></div>", unsafe_allow_html=True)
    render_charts(results, benchmark_df)
    st.markdown("<div style='height: 1rem'></div>", unsafe_allow_html=True)
    render_analytics_tables(results)


if __name__ == "__main__":
    main()
