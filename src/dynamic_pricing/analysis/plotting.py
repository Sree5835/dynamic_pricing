"""Module for plotting various statistics and data in dynamic pricing analysis."""

import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


def plot_mean_and_median_statistics_by_interval(  # pylint: disable=R0913
    mean_statistic: pd.Series,
    median_statistic: pd.Series,
    interval: int,
    x_label: str,
    y_label: str,
    title: str,
):
    """Plot both mean and median statistics by interval."""
    num_intervals = (24 * 60) // interval
    x_labels = [f"{hour:02d}" for hour in range(24)]
    plt.figure(figsize=(12, 6))
    plt.plot(
        mean_statistic,
        marker="o",
        linestyle="-",
        color="blue",
        markersize=6,
        label="Mean",
    )
    plt.plot(
        median_statistic,
        marker="x",
        linestyle="--",
        color="red",
        markersize=6,
        label="Median",
    )
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.xticks(range(0, num_intervals, 60 // interval), x_labels)
    plt.grid(True)
    plt.legend()
    plt.show()


def plot_mean_and_median_statistics_by_weekday(
    mean_statistic: pd.Series,
    median_statistic: pd.Series,
    x_label: str,
    y_label: str,
    title: str,
):
    """Plot mean and median statistics by weekday."""
    weekdays = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    mean_statistic = mean_statistic.reindex(weekdays)
    median_statistic = median_statistic.reindex(weekdays)
    plt.figure(figsize=(8, 6))
    plt.plot(
        mean_statistic,
        marker="o",
        linestyle="-",
        color="blue",
        markersize=6,
        label="Average",
    )
    plt.plot(
        median_statistic,
        marker="x",
        linestyle="--",
        color="red",
        markersize=6,
        label="Median",
    )
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.xticks(range(len(weekdays)), weekdays, rotation=45)
    plt.grid(True)
    plt.legend()
    plt.show()


def plot_items_sold(df: pd.DataFrame):
    """Plot the quantity of items sold."""
    plt.figure(figsize=(6, 12))
    plt.bar(df["item_name"], df["item_quantity"])
    plt.ylabel("Units Sold")
    plt.xlabel("Products")
    plt.title("Products Sold")
    plt.xticks(rotation=90)
    plt.subplots_adjust(left=0.5)
    plt.show()


def plot_menu_matrix(df: pd.DataFrame):
    """Plot a menu matrix based on item popularity and profitability."""
    fig = px.scatter(
        df,
        x="item_profitability",
        y="item_popularity",
        color="category",
        hover_name="item_name",
        title="Menu Matrix",
        labels={
            "item_profitability": "Profitability",
            "item_popularity": "Popularity",
        },
        color_discrete_map={
            "Star": "blue",
            "Puzzle": "green",
            "Cash Cow": "orange",
            "Dud": "red",
        },
        opacity=0.7,
    )
    for _, row in df.iterrows():
        fig.add_annotation(
            x=row["item_profitability"],
            y=row["item_popularity"],
            text=row["item_name"],
            showarrow=False,
            xshift=5,
            yshift=10,
        )
    fig.show()


def plot_profits_over_time(df: pd.DataFrame):
    """Plot profit changes over time by day period."""
    print(df.columns)
    fig = go.Figure()
    intervals = [col for col in df.columns if col != "Period"]
    for interval in intervals:
        fig.add_trace(
            go.Scatter(
                x=df["Period"],
                y=df[interval],
                mode="lines+markers",
                name=interval,
            )
        )
    fig.update_layout(
        title="Profit Changes Over Time by Day Period",
        xaxis_title="Period",
        yaxis_title="Profit",
        legend_title="Day Period",
    )
    fig.show()
