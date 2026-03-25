"""
utils/helpers.py
----------------
matplotlib chart generators.
All functions return raw PNG bytes (io.BytesIO content) so charts can
be embedded directly into both the PDF and Excel outputs without writing
temporary files to disk.
"""

import io
import logging
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend - must be set before pyplot import
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Colour palette ──────────────────────────────────────────────────────────
PALETTE = ["#2563EB", "#16A34A", "#DC2626", "#D97706", "#7C3AED",
           "#0891B2", "#DB2777", "#65A30D", "#EA580C", "#4F46E5"]
SEVERITY_COLOURS = {"HIGH": "#DC2626", "MEDIUM": "#D97706", "LOW": "#16A34A"}


def _save_fig(fig) -> bytes:
    """Render a matplotlib figure to PNG bytes and close it."""
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def time_series_chart(
    series: list,
    title: str,
    ylabel: str,
    width: float = 10,
    height: float = 4,
) -> bytes:
    """
    Generate a time-series line chart.

    Parameters
    ----------
    series : list of dicts  [{label, timestamps: [datetime], values: [float]}]
    title  : chart title
    ylabel : y-axis label
    Returns PNG bytes.
    """
    fig, ax = plt.subplots(figsize=(width, height))
    for i, s in enumerate(series):
        if not s.get("timestamps") or not s.get("values"):
            continue
        colour = PALETTE[i % len(PALETTE)]
        ax.plot(s["timestamps"], s["values"], label=s.get("label", ""), color=colour, linewidth=1.5)
    ax.set_title(title, fontsize=12, fontweight="bold", pad=10)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate(rotation=30)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    if len(series) > 1:
        ax.legend(fontsize=8, loc="upper left")
    return _save_fig(fig)


def bar_chart(
    labels: list,
    values: list,
    title: str,
    ylabel: str = "USD",
    width: float = 10,
    height: float = 5,
) -> bytes:
    """Generate a horizontal bar chart. Returns PNG bytes."""
    fig, ax = plt.subplots(figsize=(width, height))
    colours = [PALETTE[i % len(PALETTE)] for i in range(len(labels))]
    bars = ax.barh(labels, values, color=colours, edgecolor="none", height=0.6)
    ax.set_title(title, fontsize=12, fontweight="bold", pad=10)
    ax.set_xlabel(ylabel, fontsize=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    # Add value labels on bars
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_width() + bar.get_width() * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"${val:,.2f}",
            va="center",
            ha="left",
            fontsize=8,
        )
    return _save_fig(fig)


def pie_chart(
    labels: list,
    values: list,
    title: str,
    width: float = 7,
    height: float = 6,
) -> bytes:
    """Generate a pie chart. Returns PNG bytes."""
    fig, ax = plt.subplots(figsize=(width, height))
    wedges, texts, autotexts = ax.pie(
        values,
        labels=None,
        autopct="%1.1f%%",
        colors=PALETTE[: len(labels)],
        startangle=140,
        pctdistance=0.8,
    )
    for at in autotexts:
        at.set_fontsize(8)
    ax.legend(
        wedges,
        [f"{l} (${v:,.2f})" for l, v in zip(labels, values)],
        loc="lower center",
        bbox_to_anchor=(0.5, -0.15),
        ncol=2,
        fontsize=7,
    )
    ax.set_title(title, fontsize=12, fontweight="bold", pad=10)
    return _save_fig(fig)


def severity_donut(high: int, medium: int, low: int, title: str = "Security Findings") -> bytes:
    """
    Generate a professional donut chart for security finding severity counts.
    Rendered at 150 dpi in a square aspect ratio so it never appears squished
    when displayed in either PDF or Excel reports.
    Returns PNG bytes.
    """
    labels  = []
    values  = []
    colours = []
    for label, val, colour in [
        ("HIGH",   high,   "#DC2626"),
        ("MEDIUM", medium, "#D97706"),
        ("LOW",    low,    "#16A34A"),
    ]:
        if val > 0:
            labels.append(f"{label} ({val})")
            values.append(val)
            colours.append(colour)
    if not values:
        values  = [1]
        labels  = ["No findings"]
        colours = ["#9CA3AF"]

    fig, ax = plt.subplots(figsize=(6, 6))
    fig.patch.set_facecolor("white")

    wedges, _, autotexts = ax.pie(
        values,
        labels=None,
        autopct="%1.0f%%",
        colors=colours,
        startangle=90,
        pctdistance=0.78,
        wedgeprops={"width": 0.52, "edgecolor": "white", "linewidth": 2},
    )
    for at in autotexts:
        at.set_fontsize(11)
        at.set_color("white")
        at.set_fontweight("bold")

    # Legend below the donut with enough vertical clearance
    ax.legend(
        wedges, labels,
        loc="lower center",
        bbox_to_anchor=(0.5, -0.12),
        ncol=len(labels),
        fontsize=10,
        frameon=False,
    )
    ax.set_title(title, fontsize=13, fontweight="bold", pad=16, color="#1A2332")

    # Total count in the centre hole
    total = high + medium + low
    ax.text(
        0, 0, str(total),
        ha="center", va="center",
        fontsize=26, fontweight="bold", color="#1F2937",
    )

    buf = io.BytesIO()
    fig.tight_layout(pad=1.5)
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def billing_trend_chart(billing: dict, width: float = 10, height: float = 4) -> bytes:
    """
    Generate a bar chart showing total cost per time window (7d, 15d, 30d).
    Returns PNG bytes.
    """
    windows = []
    totals = []
    for window in [7, 15, 30]:
        key = f"{window}d"
        if key in billing:
            windows.append(f"Last {window} days")
            totals.append(float(billing[key].get("total", 0)))
    if not windows:
        fig, ax = plt.subplots(figsize=(width, height))
        ax.text(0.5, 0.5, "No billing data", ha="center", va="center", transform=ax.transAxes)
        return _save_fig(fig)

    fig, ax = plt.subplots(figsize=(width, height))
    bars = ax.bar(windows, totals, color=PALETTE[:3], edgecolor="none", width=0.5)
    ax.set_title("Total AWS Cost by Time Window", fontsize=12, fontweight="bold", pad=10)
    ax.set_ylabel("USD", fontsize=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    for bar, val in zip(bars, totals):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(totals) * 0.01,
                f"${val:,.2f}", ha="center", va="bottom", fontsize=10, fontweight="bold")
    return _save_fig(fig)
