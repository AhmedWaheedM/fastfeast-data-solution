import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

import log_parser as lp
from theme import (
    LEVEL_COLOURS,
    LEVEL_BADGE_COLOURS,
    PLOT_BG,
    PAPER_BG,
    FONT_COLOR,
    GRID_COLOR,
    HEADER_COLOR,
)

st.set_page_config(
    page_title="FastFeast Log Reporter",
    page_icon="🍔",
    layout="wide",
    initial_sidebar_state="expanded",
)


def level_badge(level):
    bg, fg = LEVEL_BADGE_COLOURS.get(level, ("bg:#dee2e6", "#343a40"))
    return (
        f'<span style="background:{bg.replace("bg:","")};color:{fg};'
        f'padding:1px 6px;border-radius:4px;font-size:0.75rem;'
        f'font-weight:600;font-family:monospace">{level}</span>'
    )


def status_card(label, value, colour="#0d6efd", icon=""):
    st.markdown(
        f"""
        <div style="background:#1e1e2e;border-radius:10px;padding:14px 18px;
                    border-left:4px solid {colour};margin-bottom:4px">
          <div style="color:#8b8fa8;font-size:0.75rem;font-weight:600;
                      text-transform:uppercase;letter-spacing:.06em">{icon} {label}</div>
          <div style="color:#cdd6f4;font-size:1.55rem;font-weight:700;
                      margin-top:4px">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_log_table(table):
    if table.num_rows == 0:
        st.info("No log entries match the current filters.")
        return

    timestamps   = table.column("timestamp").to_pylist()
    levels       = table.column("level").to_pylist()
    loggers      = table.column("logger").to_pylist()
    messages     = table.column("message").to_pylist()
    source_files = table.column("source_file").to_pylist()

    html_rows = []
    for ts_raw, level, logger, msg, src in zip(
        timestamps, levels, loggers, messages, source_files
    ):
        ts    = str(ts_raw)[:19].replace("T", " ") if ts_raw else ""
        badge = level_badge(level or "INFO")
        html_rows.append(
            f"<tr>"
            f"<td style='white-space:nowrap;color:#8b8fa8;font-size:.78rem;padding:4px 8px'>{ts}</td>"
            f"<td style='padding:4px 6px'>{badge}</td>"
            f"<td style='color:#a6e3a1;font-size:.78rem;padding:4px 8px;white-space:nowrap'>{logger or ''}</td>"
            f"<td style='color:#cdd6f4;font-size:.82rem;padding:4px 8px'>{msg or ''}</td>"
            f"<td style='color:#6c7086;font-size:.72rem;padding:4px 8px;white-space:nowrap'>{src or ''}</td>"
            f"</tr>"
        )

    table_html = (
        "<div style='overflow-x:auto;max-height:480px;overflow-y:auto'>"
        "<table style='width:100%;border-collapse:collapse;font-family:monospace'>"
        f"<thead><tr style='background:#313244;position:sticky;top:0;z-index:1'>"
        f"<th style='padding:6px 8px;color:{HEADER_COLOR};font-size:.78rem;text-align:left'>Timestamp</th>"
        f"<th style='padding:6px 8px;color:{HEADER_COLOR};font-size:.78rem;text-align:left'>Level</th>"
        f"<th style='padding:6px 8px;color:{HEADER_COLOR};font-size:.78rem;text-align:left'>Logger</th>"
        f"<th style='padding:6px 8px;color:{HEADER_COLOR};font-size:.78rem;text-align:left'>Message</th>"
        f"<th style='padding:6px 8px;color:{HEADER_COLOR};font-size:.78rem;text-align:left'>File</th>"
        "</tr></thead>"
        "<tbody>" + "".join(html_rows) + "</tbody></table></div>"
    )
    st.markdown(table_html, unsafe_allow_html=True)


with st.sidebar:
    st.markdown("## 🍔 FastFeast Reporter")
    st.divider()

    default_root   = str(Path(__file__).parent.parent.parent)
    log_root_input = st.text_input(
        "Project root folder",
        value=default_root,
        help="Root of your FastFeast project (contains FastFeast/ and data/ folders)",
    )

    st.divider()
    st.markdown("### ⚙️ Refresh")
    auto_refresh     = st.toggle("Auto-refresh", value=True)
    refresh_interval = st.select_slider(
        "Interval (seconds)",
        options=[5, 10, 15, 30, 60],
        value=10,
        disabled=not auto_refresh,
    )

    st.divider()
    st.markdown("### 🔍 Log Filters")
    min_level      = st.selectbox(
        "Minimum level",
        options=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        index=1,
    )
    logger_filter  = st.text_input("Logger name contains", "")
    message_filter = st.text_input("Message contains", "")
    max_rows       = st.slider("Max rows shown", 50, 1000, 200, step=50)

    st.divider()
    st.markdown("### 📂 Log Files")
    show_all_files = st.toggle(
        "All log files", value=False,
        help="When off, only the newest log file is loaded",
    )

    st.divider()
    st.caption("FastFeast Real-Time Reporter v1.0")


if auto_refresh:
    st_autorefresh(interval=refresh_interval * 1_000, key="auto_refresh_counter")


project_root = Path(log_root_input)
log_root     = project_root / "logs"
data_root    = project_root / "data" / "bronze"

log_files_found    = lp.find_log_files(str(log_root))
metric_files_found = lp.find_metric_files(str(log_root))
jsonl_files_found  = lp.find_jsonl_files(str(log_root))


st.markdown(
    """
    <div style="display:flex;align-items:center;gap:14px;margin-bottom:6px">
      <span style="font-size:2.2rem">🍔</span>
      <div>
        <h1 style="margin:0;font-size:1.6rem;color:#cdd6f4">FastFeast Log Reporter</h1>
        <span style="color:#8b8fa8;font-size:.85rem">Real-time pipeline observability dashboard</span>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

last_refresh = time.strftime("%H:%M:%S")
st.caption(f"Last refreshed: {last_refresh}  |  Project: `{project_root}`")
st.divider()

if not log_root.exists():
    st.error(
        f"Log directory not found: `{log_root}`\n\n"
        "Please update the **Project root folder** in the sidebar."
    )
    st.stop()


@st.cache_data(ttl=refresh_interval if auto_refresh else 3600, show_spinner=False)
def load_logs(files_to_load, tail):
    return lp.parse_multiple_log_files(list(files_to_load), tail_per_file=tail)


@st.cache_data(ttl=refresh_interval if auto_refresh else 3600, show_spinner=False)
def load_metrics(files_to_load):
    metrics_list = lp.load_all_metrics(list(files_to_load))
    summary_tbl  = lp.metrics_summary_table(metrics_list)
    per_file_tbl = lp.per_file_issues_table(metrics_list)
    return summary_tbl, per_file_tbl


files_to_use = (
    tuple(log_files_found)
    if show_all_files
    else tuple(log_files_found[:1])
)

with st.spinner("Loading logs…"):
    raw_table                    = load_logs(files_to_use, tail=max_rows * 2)
    summary_tbl, per_file_tbl   = load_metrics(tuple(metric_files_found))

filtered_table = lp.filter_log_table(
    raw_table,
    min_level      = min_level,
    logger_filter  = logger_filter,
    message_filter = message_filter,
    last_n         = max_rows,
)

level_counts = lp.count_by_level(raw_table)


c1, c2, c3, c4, c5, c6 = st.columns(6)

with c1:
    status_card("Total Entries", f"{raw_table.num_rows:,}", "#89b4fa", "📋")
with c2:
    status_card("Errors", level_counts.get("ERROR", 0), LEVEL_COLOURS["ERROR"], "🔴")
with c3:
    status_card("Warnings", level_counts.get("WARNING", level_counts.get("WARN", 0)), LEVEL_COLOURS["WARNING"], "🟡")
with c4:
    if summary_tbl.num_rows > 0:
        status_card("Files Validated", int(summary_tbl.column("files_total")[0].as_py()), "#89dceb", "📂")
    else:
        status_card("Files Validated", "—", "#89dceb", "📂")
with c5:
    if summary_tbl.num_rows > 0:
        cr     = summary_tbl.column("clean_rate")[0].as_py()
        colour = LEVEL_COLOURS["INFO"] if cr >= 80 else LEVEL_COLOURS["WARNING"] if cr >= 50 else LEVEL_COLOURS["ERROR"]
        status_card("Clean Rate", f"{cr:.1f}%", colour, "✅")
    else:
        status_card("Clean Rate", "—", LEVEL_COLOURS["INFO"], "✅")
with c6:
    status_card("Log Files Found", len(log_files_found), "#cba6f7", "🗂️")

st.divider()


tab_live, tab_metrics, tab_files = st.tabs([
    "📡 Live Log Stream",
    "📊 Validation Metrics",
    "📂 File Browser",
])


with tab_live:
    col_log, col_chart = st.columns([3, 1])

    with col_chart:
        st.markdown("#### Level Breakdown")
        if level_counts:
            lc_table = pa.table({
                "Level": pa.array(list(level_counts.keys()),   type=pa.string()),
                "Count": pa.array(list(level_counts.values()), type=pa.int64()),
            })
            sort_idx = pc.sort_indices(lc_table, sort_keys=[("Count", "ascending")])
            lc_table = lc_table.take(sort_idx)

            level_list  = lc_table.column("Level").to_pylist()
            count_list  = lc_table.column("Count").to_pylist()
            bar_colours = [LEVEL_COLOURS.get(lv, "#cdd6f4") for lv in level_list]

            fig = go.Figure(go.Bar(
                x            = count_list,
                y            = level_list,
                orientation  = "h",
                marker_color = bar_colours,
                text         = count_list,
                textposition = "outside",
            ))
            fig.update_layout(
                plot_bgcolor  = PLOT_BG,
                paper_bgcolor = PAPER_BG,
                font_color    = FONT_COLOR,
                margin        = dict(l=0, r=20, t=10, b=10),
                height        = 220,
                xaxis         = dict(showgrid=False, showticklabels=False),
                yaxis         = dict(showgrid=False),
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("No log data yet.")

        st.markdown("#### Timeline")
        if raw_table.num_rows > 0 and "timestamp" in raw_table.schema.names:
            ts_df = raw_table.select(["timestamp", "level"]).to_pandas()
            ts_df = ts_df.dropna(subset=["timestamp"])
            if not ts_df.empty:
                timeline = (
                    ts_df.set_index("timestamp")
                         .resample("1min")["level"]
                         .count()
                         .reset_index()
                         .rename(columns={"level": "count"})
                )
                if not timeline.empty:
                    fig2 = px.area(
                        timeline, x="timestamp", y="count",
                        color_discrete_sequence=[LEVEL_COLOURS["INFO"]],
                    )
                    fig2.update_layout(
                        plot_bgcolor  = PLOT_BG,
                        paper_bgcolor = PAPER_BG,
                        font_color    = FONT_COLOR,
                        margin        = dict(l=0, r=0, t=10, b=10),
                        height        = 160,
                        xaxis_title   = None,
                        yaxis_title   = "events/min",
                        xaxis         = dict(showgrid=False),
                        yaxis         = dict(showgrid=True, gridcolor=GRID_COLOR),
                    )
                    st.plotly_chart(fig2, use_container_width=True,
                                    config={"displayModeBar": False})

    with col_log:
        n_showing = filtered_table.num_rows
        n_total   = raw_table.num_rows
        st.markdown(
            f"#### Log Entries  "
            f"<span style='color:#8b8fa8;font-size:.85rem'>"
            f"showing {n_showing:,} / {n_total:,}</span>",
            unsafe_allow_html=True,
        )

        if log_files_found:
            newest = Path(log_files_found[0]).name
            st.caption(
                f"Source: `{newest}`"
                + (f" + {len(files_to_use)-1} more" if len(files_to_use) > 1 else "")
            )

        render_log_table(filtered_table)

        if raw_table.num_rows > 0:
            export_cols = [c for c in filtered_table.schema.names
                           if c not in ("level_rank", "colour")]
            csv_bytes = (
                filtered_table.select(export_cols)
                              .to_pandas()
                              .to_csv(index=False)
            )
            st.download_button(
                "⬇️ Download filtered log as CSV",
                data      = csv_bytes,
                file_name = f"fastfeast_log_{time.strftime('%Y%m%d_%H%M%S')}.csv",
                mime      = "text/csv",
            )


with tab_metrics:
    if summary_tbl.num_rows == 0:
        st.info("No validation metric files found under the log directory.")
    else:
        st.markdown("#### Run Summary")
        display_cols = [
            "run_id", "recorded_at", "files_total",
            "files_clean", "files_failed", "total_rows",
            "total_issues", "clean_rate",
        ]

        show_df               = summary_tbl.select(display_cols).to_pandas()
        show_df["recorded_at"] = show_df["recorded_at"].dt.strftime("%Y-%m-%d %H:%M:%S")

        st.dataframe(
            show_df.style
                .format({"clean_rate": "{:.1f}%"})
                .background_gradient(subset=["clean_rate"],   cmap="RdYlGn", vmin=0, vmax=100)
                .background_gradient(subset=["total_issues"], cmap="Reds"),
            use_container_width=True,
            hide_index=True,
        )

        if per_file_tbl.num_rows > 0:
            st.markdown("#### Per-File Issue Breakdown")

            issue_types = [
                "null_issues", "type_issues", "enum_issues",
                "range_issues", "format_issues", "missing_cols",
            ]

            agg_table = (
                per_file_tbl.group_by("file_name")
                            .aggregate([
                                ("null_issues",   "sum"),
                                ("type_issues",   "sum"),
                                ("enum_issues",   "sum"),
                                ("range_issues",  "sum"),
                                ("format_issues", "sum"),
                                ("missing_cols",  "sum"),
                                ("total_rows",    "sum"),
                                ("issue_count",   "sum"),
                            ])
            )
            rename_map = {
                "null_issues_sum":   "null_issues",
                "type_issues_sum":   "type_issues",
                "enum_issues_sum":   "enum_issues",
                "range_issues_sum":  "range_issues",
                "format_issues_sum": "format_issues",
                "missing_cols_sum":  "missing_cols",
                "total_rows_sum":    "total_rows",
                "issue_count_sum":   "issue_count",
            }
            for old, new in rename_map.items():
                if old in agg_table.schema.names:
                    idx       = agg_table.schema.get_field_index(old)
                    agg_table = agg_table.rename_columns(
                        [new if i == idx else agg_table.schema.names[i]
                         for i in range(len(agg_table.schema.names))]
                    )

            sort_idx  = pc.sort_indices(agg_table, sort_keys=[("issue_count", "descending")])
            agg_table = agg_table.take(sort_idx)
            agg       = agg_table.to_pandas()

            col_bar, col_tbl = st.columns([1, 1])

            with col_bar:
                fig3 = px.bar(
                    agg[agg["issue_count"] > 0].head(10),
                    x      = "file_name",
                    y      = issue_types,
                    title  = "Issues by File & Type",
                    color_discrete_sequence = px.colors.qualitative.Pastel,
                    labels = {"value": "Issues", "variable": "Type", "file_name": "File"},
                )
                fig3.update_layout(
                    plot_bgcolor      = PLOT_BG,
                    paper_bgcolor     = PAPER_BG,
                    font_color        = FONT_COLOR,
                    legend_title_text = "Issue Type",
                    margin            = dict(l=0, r=0, t=40, b=0),
                    xaxis             = dict(showgrid=False),
                    yaxis             = dict(showgrid=True, gridcolor=GRID_COLOR),
                )
                st.plotly_chart(fig3, use_container_width=True,
                                config={"displayModeBar": False})

            with col_tbl:
                st.dataframe(
                    agg.style.background_gradient(subset=["issue_count"], cmap="Reds"),
                    use_container_width=True,
                    hide_index=True,
                    height=320,
                )

        st.markdown("#### Clean Rate Trend")
        if summary_tbl.num_rows > 1:
            sort_idx  = pc.sort_indices(summary_tbl, sort_keys=[("recorded_at", "ascending")])
            trend_tbl = summary_tbl.take(sort_idx)
            trend_df  = trend_tbl.select(["recorded_at", "clean_rate"]).to_pandas()

            fig4 = px.line(
                trend_df, x="recorded_at", y="clean_rate",
                markers                 = True,
                color_discrete_sequence = [LEVEL_COLOURS["INFO"]],
                labels                  = {"recorded_at": "Run time", "clean_rate": "Clean rate (%)"},
            )
            fig4.update_layout(
                plot_bgcolor  = PLOT_BG,
                paper_bgcolor = PAPER_BG,
                font_color    = FONT_COLOR,
                yaxis         = dict(range=[0, 105], showgrid=True, gridcolor=GRID_COLOR),
                xaxis         = dict(showgrid=False),
                margin        = dict(l=0, r=0, t=10, b=0),
                height        = 220,
            )
            fig4.add_hline(
                y                = 95,
                line_dash        = "dash",
                line_color       = LEVEL_COLOURS["ERROR"],
                annotation_text  = "95% target",
            )
            st.plotly_chart(fig4, use_container_width=True,
                            config={"displayModeBar": False})
        else:
            st.info("Run more validation passes to see the trend.")


with tab_files:
    st.markdown("#### Discovered Log Files")

    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("**`.log` files**")
        if log_files_found:
            for p in log_files_found:
                size_kb = os.path.getsize(p) / 1024
                mtime   = time.strftime("%Y-%m-%d %H:%M", time.localtime(os.path.getmtime(p)))
                rel     = str(Path(p).relative_to(project_root))
                st.markdown(
                    f"- `{rel}`  "
                    f"<span style='color:#8b8fa8;font-size:.78rem'>"
                    f"{size_kb:.1f} KB | {mtime}</span>",
                    unsafe_allow_html=True,
                )
        else:
            st.info("No .log files found.")

    with col_r:
        st.markdown("**Validation metric `.json` files**")
        if metric_files_found:
            for p in metric_files_found:
                size_kb = os.path.getsize(p) / 1024
                mtime   = time.strftime("%Y-%m-%d %H:%M", time.localtime(os.path.getmtime(p)))
                rel     = str(Path(p).relative_to(project_root))
                st.markdown(
                    f"- `{rel}`  "
                    f"<span style='color:#8b8fa8;font-size:.78rem'>"
                    f"{size_kb:.1f} KB | {mtime}</span>",
                    unsafe_allow_html=True,
                )
        else:
            st.info("No metric JSON files found.")

    if log_files_found:
        st.divider()
        st.markdown("#### Inspect a Log File")
        selected = st.selectbox(
            "Select file to inspect",
            options      = log_files_found,
            format_func  = lambda p: Path(p).name,
        )
        if selected:
            inspect_tbl = lp.parse_log_file(selected)
            st.caption(
                f"`{selected}` — {inspect_tbl.num_rows} entries  |  "
                f"{os.path.getsize(selected)/1024:.1f} KB"
            )
            render_log_table(inspect_tbl.slice(0, 300))