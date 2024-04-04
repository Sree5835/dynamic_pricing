import pandas as pd
import matplotlib.pyplot as plt
import itertools
import numpy as np
from datetime import timedelta
import plotly.graph_objects as go
import plotly.express as px


order_timestamp = "order_placed_timestamp"


def split_weekdays_and_weekends(df: pd.DataFrame, time_col: str):
    weekdays_df = df[df[time_col].dt.dayofweek < 5]
    weekend_df = df[df[time_col].dt.dayofweek >= 5]
    return weekdays_df, weekend_df


def calculate_revenue(df: pd.DataFrame):
    # Calculate the actual average revenue for each interval on an average day
    df = df[
        [
            "order_id",
            "item_quantity",
            "item_fractional_price",
            "modifier_fractional_price",
            "modifier_quantity",
            order_timestamp,
        ]
    ].copy()
    df.fillna(0, inplace=True)
    df.loc[:, "order_value"] = (
        (df["item_fractional_price"] * df["item_quantity"])
        + (df["modifier_fractional_price"] * df["modifier_quantity"])
    ) / 100
    # print(df.groupby("order_id").head())
    df.loc[:, "revenue"] = df.groupby("order_id")["order_value"].transform("sum")
    return df


def plot_mean_and_median_statistics_by_interval(
    mean_statistic: pd.DataFrame,
    median_statistic: pd.DataFrame,
    interval: int,
    x_label: str,
    y_label: str,
    title: str,
):
    # Calculate the total number of intervals in a day based on the given interval
    num_intervals = (24 * 60) // interval

    # Generate the x-axis labels for the hour of the day
    x_labels = [f"{hour:02d}" for hour in range(24)]

    # Plotting both mean and median
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
    mean_statistic: pd.DataFrame,
    median_statistic: pd.DataFrame,
    x_label: str,
    y_label: str,
    title: str,
):
    # Define the order of days for proper sorting in the plot
    weekdays = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]

    # Reindex the DataFrames to include all weekdays in the desired order
    mean_statistic = mean_statistic.reindex(weekdays)
    median_statistic = median_statistic.reindex(weekdays)

    # Generate the x-axis labels for the day of the week
    x_labels = weekdays

    # Plotting both mean and median number of orders by day of the week as a line plot
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
    plt.xticks(range(len(weekdays)), x_labels, rotation=45)
    plt.grid(True)
    plt.legend()
    plt.show()


def plot_average_orders_per_interval(df: pd.DataFrame, interval: int):

    # Create a new column for the interval index
    df.loc[:, "interval_index"] = (
        df[order_timestamp].dt.hour * 60 + df[order_timestamp].dt.minute
    ) // interval

    # Calculate the actual average number of orders for each interval on an average day
    actual_average_orders = (
        df.groupby(["interval_index", df[order_timestamp].dt.date])
        .size()
        .groupby("interval_index")
        .mean()
    )

    # Calculate the median number of orders for each interval on an average day
    median_orders = (
        df.groupby(["interval_index", df[order_timestamp].dt.date])
        .size()
        .groupby("interval_index")
        .median()
    )

    plot_mean_and_median_statistics_by_interval(
        actual_average_orders,
        median_orders,
        interval,
        "Hour of the Day",
        "Number of Orders",
        f"Mean and Median Number of Orders per {interval}-min on an Average Day",
    )

    return actual_average_orders, median_orders


def plot_average_revenue_per_interval(df: pd.DataFrame, interval: int):

    df = calculate_revenue(df)
    # print(df["revenue"])
    print(df.head())
    # Create a new column for the interval index
    df.loc[:, "interval_index"] = (
        df[order_timestamp].dt.hour * 60 + df[order_timestamp].dt.minute
    ) // interval

    # Calculate the mean and median revenue for each interval
    mean_revenue = (
        df.groupby(["interval_index", df[order_timestamp].dt.date])["revenue"]
        .sum()
        .groupby("interval_index")
        .mean()
    )

    median_revenue = (
        df.groupby(["interval_index", df[order_timestamp].dt.date])["revenue"]
        .sum()
        .groupby("interval_index")
        .median()
    )

    plot_mean_and_median_statistics_by_interval(
        mean_revenue,
        median_revenue,
        interval,
        "Hour of the Day",
        "Average Revenue",
        f"Mean and Median Revenue per {interval}-min on an Average Day",
    )
    return mean_revenue, median_revenue


def plot_items_sold(df):
    df = df[["order_id", "item_name", "item_quantity"]].copy()

    plt.figure(figsize=(6, 12))
    plt.bar(df["item_name"], df["item_quantity"])
    plt.ylabel("Products")
    plt.xlabel("Units Sold")
    plt.title("Products Sold")
    plt.xticks(rotation=90)
    plt.subplots_adjust(left=0.5)  # Adjust the value as needed
    plt.show()


def plot_average_orders_by_day_of_week(df: pd.DataFrame):
    # Extract the day of the week from the order_datetime column
    df.loc[:, "day_of_week"] = df[order_timestamp].dt.day_name()

    # Group by day of the week and calculate both mean and median number of orders
    average_orders = (
        df.groupby(["day_of_week", df[order_timestamp].dt.date])
        .size()
        .groupby("day_of_week")
        .mean()
    )
    median_orders = (
        df.groupby(["day_of_week", df[order_timestamp].dt.date])
        .size()
        .groupby("day_of_week")
        .median()
    )

    plot_mean_and_median_statistics_by_weekday(
        average_orders,
        median_orders,
        "Day of the Week",
        "Number of Orders",
        "Mean and Median Number of Orders by Day of the Week",
    )
    return average_orders, median_orders


def plot_average_revenue_by_day_of_week(df: pd.DataFrame):
    # Calculate the average revenue by day of the week
    df = calculate_revenue(df)

    # Extract the day of the week from the order_datetime column
    df.loc[:, "day_of_week"] = df[order_timestamp].dt.day_name()

    average_revenue_by_day = (
        df.groupby(["day_of_week", df[order_timestamp].dt.date])["revenue"]
        .sum()
        .groupby("day_of_week")
        .mean()
    )
    median_revenue_by_day = (
        df.groupby(["day_of_week", df[order_timestamp].dt.date])["revenue"]
        .sum()
        .groupby("day_of_week")
        .median()
    )

    plot_mean_and_median_statistics_by_weekday(
        average_revenue_by_day,
        median_revenue_by_day,
        "Day of the Week",
        "Revenue",
        "Mean and Median Revenue by Day of the Week",
    )
    return average_revenue_by_day, median_revenue_by_day


def time_difference_in_order_acceptance_per_interval(df: pd.DataFrame, interval: int):
    accepted_timestamp = "order_updated_timestamp"

    # Create a new column for the interval index
    df.loc[:, "interval_index"] = (
        df[order_timestamp].dt.hour * 60 + df[order_timestamp].dt.minute
    ) // interval

    # Calculate the time difference between order placement and acceptance
    df.loc[:, "time_difference"] = (
        df[accepted_timestamp] - df[order_timestamp]
    ).dt.total_seconds() / 60

    # Calculate both mean and median time difference for each interval on an average day
    mean_time_difference = (
        df.groupby(["interval_index", df[order_timestamp].dt.date])["time_difference"]
        .mean()
        .groupby("interval_index")
        .mean()
    )

    median_time_difference = (
        df.groupby(["interval_index", df[order_timestamp].dt.date])["time_difference"]
        .median()
        .groupby("interval_index")
        .median()
    )

    plot_mean_and_median_statistics_by_interval(
        mean_time_difference,
        median_time_difference,
        interval,
        "Hour of the Day",
        "Time Difference in Order Acceptance (minutes)",
        f"Mean and Median Time Difference in Order Acceptance per {interval}-min on an Average Day",
    )

    return mean_time_difference, median_time_difference


def prep_time_per_interval(df: pd.DataFrame, interval: int):
    """Use https://api-docs.deliveroo.com/v2.0/docs/order-integration to understand why these timestamps are used"""
    start_prep_time = "order_start_prepping_at_timestamp"
    end_prep_time = "order_prepare_for_timestamp"

    # Create a new column for the interval index
    df.loc[:, "interval_index"] = (
        df[start_prep_time].dt.hour * 60 + df[start_prep_time].dt.minute
    ) // interval

    # Calculate the time difference between order placement and acceptance
    df.loc[:, "time_difference"] = (
        df[end_prep_time] - df[start_prep_time]
    ).dt.total_seconds() / 60

    # Calculate both mean and median time difference for each interval on an average day
    mean_time_difference = (
        df.groupby(["interval_index", df[start_prep_time].dt.date])["time_difference"]
        .mean()
        .groupby("interval_index")
        .mean()
    )

    median_time_difference = (
        df.groupby(["interval_index", df[start_prep_time].dt.date])["time_difference"]
        .median()
        .groupby("interval_index")
        .median()
    )

    plot_mean_and_median_statistics_by_interval(
        mean_time_difference,
        median_time_difference,
        interval,
        "Hour of the Day",
        "Prep Time Difference (minutes)",
        f"Mean and Median Prep Time per {interval}-min on an Average Day",
    )
    return mean_time_difference, median_time_difference


def calculate_revenue_by_day_period(df, time_intervals=None):

    if time_intervals is None:
        raise ValueError("Please provide time intervals.")

    df.loc[:, "order_value"] = (
        (df["item_fractional_price"] * df["item_quantity"])
        + (df["modifier_fractional_price"] * df["modifier_quantity"])
    ) / 100
    time_intervals = [pd.to_datetime(str(time)).time() for time in time_intervals]

    interval_labels = [
        f"{time_intervals[i]} to {time_intervals[i+1]}"
        for i in range(len(time_intervals) - 1)
    ]

    df.loc[:, "interval_label"] = pd.cut(
        df[order_timestamp].dt.time, bins=time_intervals, labels=interval_labels
    )

    return df.groupby("interval_label", observed=True)["order_value"].sum()


def calculate_profit_by_day_period(df, time_intervals=None):
    if time_intervals is None:
        raise ValueError("Please provide time intervals.")

    df.loc[:, "order_value"] = (
        (df["item_fractional_price"] * df["item_quantity"])
        + (df["modifier_fractional_price"] * df["modifier_quantity"])
    ) / 100

    df.loc[:, "profit"] = df["order_value"] - (df["item_fractional_cost"] / 100)

    time_intervals = [pd.to_datetime(str(time)).time() for time in time_intervals]

    interval_labels = [
        f"{time_intervals[i]} to {time_intervals[i+1]}"
        for i in range(len(time_intervals) - 1)
    ]

    df.loc[:, "interval_label"] = pd.cut(
        df[order_timestamp].dt.time, bins=time_intervals, labels=interval_labels
    )

    return df.groupby("interval_label", observed=True)["profit"].sum()


def calculate_profits_over_periods(df, time_intervals=None):
    # Ensure df is sorted by order_timestamp
    df.sort_values(by=order_timestamp, inplace=True)

    # Find the earliest and latest dates
    start_date = df[order_timestamp].min()
    end_date = df[order_timestamp].max()

    # Initialize the results DataFrame
    profit_results = pd.DataFrame()

    # Calculate the total period covered by the dataframe
    total_days = (end_date - start_date).days
    periods = total_days // 21  # 3 weeks * 7 days

    # Loop through each period
    for period in range(
        periods + 1
    ):  # +1 to include the last period which may be < 3 weeks
        period_start = start_date + timedelta(weeks=3 * period)
        period_end = min(period_start + timedelta(weeks=3), end_date)
        period_df = df[
            (df[order_timestamp] >= period_start) & (df[order_timestamp] < period_end)
        ]

        # Check if the period has orders over 90% of the days
        unique_order_days = period_df[order_timestamp].dt.date.nunique()
        if unique_order_days >= 5:  # At least 19 days with orders in a 21-day period

            # Calculate profits for this period
            period_profit = calculate_profit_by_day_period(period_df, time_intervals)
            period_profit["Period"] = period + 1
            # Append the period's profits to the results DataFrame
            profit_results = pd.concat(
                [profit_results, period_profit.to_frame(name=f"Period {period + 1}").T]
            )
        else:
            # Handle periods with insufficient data (optional, based on your needs)
            print(
                f"Period {period + 1} skipped due to insufficient order days ({unique_order_days} days)"
            )

    profit_results.reset_index(drop=True, inplace=True)
    return profit_results


def plot_profits_over_time(df):
    # Print the DataFrame columns for debugging
    print(df.columns)

    fig = go.Figure()

    # Exclude the 'Period' column from the columns to iterate over
    # This assumes 'Period' is the name of the column to exclude
    intervals = [col for col in df.columns if col != "Period"]

    # Iterate over the intervals (all columns except 'Period')
    for interval in intervals:
        fig.add_trace(
            go.Scatter(
                x=df["Period"],
                y=df[interval],
                mode="lines+markers",
                name=interval,
            )
        )

    # Update the layout to add titles and adjust axis labels
    fig.update_layout(
        title="Profit Changes Over Time by Day Period",
        xaxis_title="Period",
        yaxis_title="Profit",
        legend_title="Day Period",
    )

    # Display the figure
    fig.show()


def generate_menu_matrix(df: pd.DataFrame):
    # Calculate profit for each item

    #!NOTE: THIS IS SIMPLIFIED BECAUSE THERE ARE NO MODIFIERS
    df["item_revenue"] = (df["item_quantity"] * df["item_fractional_price"]) / 100
    df["item_cost"] = (df["item_quantity"] * df["item_fractional_cost"]) / 100

    # Aggregate data to calculate popularity and profitability(profit-margin) for each item
    item_popularity = df.groupby("item_name")["item_quantity"].sum()
    item_revenue = df.groupby("item_name")["item_revenue"].sum()
    item_cost = df.groupby("item_name")["item_cost"].sum()
    item_profitability = (item_revenue - item_cost) / item_revenue

    # Calculate thresholds for popularity and profitability
    popularity_threshold = item_popularity.quantile(0.5)  # Adjust as needed
    profitability_threshold = item_profitability.quantile(0.5)  # Adjust as needed

    # Categorize items
    def categorize_item(row):
        item_name = row["item_name"]
        popularity = item_popularity.get(item_name, 0)
        profitability = item_profitability.get(item_name, 0)

        if (
            popularity >= popularity_threshold
            and profitability >= profitability_threshold
        ):
            return "Star"
        elif (
            popularity < popularity_threshold
            and profitability >= profitability_threshold
        ):
            return "Puzzle"
        elif (
            popularity >= popularity_threshold
            and profitability < profitability_threshold
        ):
            return "Cash Cow"
        else:
            return "Dud"

    df["category"] = df.apply(categorize_item, axis=1)
    # Adding popularity and profitability to the dataframe
    df["item_popularity"] = df["item_name"].map(item_popularity)
    df["item_profitability"] = df["item_name"].map(item_profitability)

    return df


def plot_menu_matrix(df):
    fig = px.scatter(
        df,
        x="item_profitability",
        y="item_popularity",
        color="category",
        hover_name="item_name",
        title="Menu Matrix",
        labels={"item_profitability": "Profitability", "item_popularity": "Popularity"},
        color_discrete_map={
            "Star": "blue",
            "Puzzle": "green",
            "Cash Cow": "orange",
            "Dud": "red",
        },
        opacity=0.7,
    )

    # Add text annotations next to each point
    for i, row in df.iterrows():
        fig.add_annotation(
            x=row["item_profitability"],
            y=row["item_popularity"],
            text=row["item_name"],
            showarrow=False,
            font=dict(size=8),
            xshift=5,
            yshift=10,
        )

    fig.show()
