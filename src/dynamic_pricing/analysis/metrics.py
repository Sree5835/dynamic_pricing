"""Module for analyzing and plotting dynamic pricing metrics."""

from datetime import timedelta
from typing import List

import pandas as pd
from dynamic_pricing.analysis.plotting import (
    plot_statistics_with_interval,
    plot_mean_and_median_statistics_by_weekday,
    plot_menu_matrix,
    plot_items_sold,
    plot_profits_over_time,
)

ORDER_TIMESTAMP = "order_placed_timestamp"


def split_weekdays_and_weekends(df: pd.DataFrame, time_col: str) -> tuple:
    """Split the DataFrame into weekdays and weekends."""
    weekdays_df = df[df[time_col].dt.dayofweek < 5]
    weekend_df = df[df[time_col].dt.dayofweek >= 5]
    return weekdays_df, weekend_df


def calculate_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the revenue for each order."""
    df = df[
        [
            "order_id",
            "item_quantity",
            "item_fractional_price",
            "modifier_fractional_price",
            "modifier_quantity",
            ORDER_TIMESTAMP,
        ]
    ].copy()
    df.fillna(0, inplace=True)
    df.loc[:, "order_value"] = (
        (df["item_fractional_price"] * df["item_quantity"])
        + (df["modifier_fractional_price"] * df["modifier_quantity"])
    ) / 100
    df.loc[:, "revenue"] = df.groupby("order_id")["order_value"].transform(
        "sum"
    )
    return df


def calculate_average_orders_per_interval(
    df: pd.DataFrame, interval: int, plot=False
):
    """Calculate average orders per interval and optionally plot the results."""
    df[ORDER_TIMESTAMP] = pd.to_datetime(df[ORDER_TIMESTAMP])
    df.loc[:, "interval_index"] = (
        df[ORDER_TIMESTAMP].dt.hour * 60 + df[ORDER_TIMESTAMP].dt.minute
    ) // interval

    mean_orders = (
        df.groupby(["interval_index", df[ORDER_TIMESTAMP].dt.date])
        .size()
        .groupby("interval_index")
        .mean()
    )
    median_orders = (
        df.groupby(["interval_index", df[ORDER_TIMESTAMP].dt.date])
        .size()
        .groupby("interval_index")
        .median()
    )
    std_orders = (
        df.groupby(["interval_index", df[ORDER_TIMESTAMP].dt.date])
        .size()
        .groupby("interval_index")
        .std()
    )

    if plot:
        plot_statistics_with_interval(
            mean_orders,
            median_orders,
            std_orders,
            "Hour of the Day",
            "Number of Orders",
            f"Mean and Median Number of Orders per {interval}-min on an Average Day",
        )
        return None
    return mean_orders, median_orders


def calculate_average_revenue_per_interval(
    df: pd.DataFrame, interval: int, plot=False
):
    """Calculate average revenue per interval and optionally plot the results."""
    df[ORDER_TIMESTAMP] = pd.to_datetime(df[ORDER_TIMESTAMP])
    df = calculate_revenue(df)
    df.loc[:, "interval_index"] = (
        df[ORDER_TIMESTAMP].dt.hour * 60 + df[ORDER_TIMESTAMP].dt.minute
    ) // interval
    mean_revenue = (
        df.groupby(["interval_index", df[ORDER_TIMESTAMP].dt.date])[
            "order_value"
        ]
        .sum()
        .groupby("interval_index")
        .mean()
    )
    median_revenue = (
        df.groupby(["interval_index", df[ORDER_TIMESTAMP].dt.date])[
            "order_value"
        ]
        .sum()
        .groupby("interval_index")
        .median()
    )
    std_revenue = (
        df.groupby(["interval_index", df[ORDER_TIMESTAMP].dt.date])[
            "order_value"
        ]
        .sum()
        .groupby("interval_index")
        .std()
    )
    if plot:
        plot_statistics_with_interval(
            mean_revenue,
            median_revenue,
            std_revenue,
            "Hour of the Day",
            "Average Revenue",
            f"Mean and Median Revenue per {interval}-min on an Average Day",
        )
        return None
    return mean_revenue, median_revenue


def calculate_time_difference_in_order_acceptance_per_interval(
    df: pd.DataFrame, interval: int, plot=False
):
    """Calculate time difference in order acceptance per interval and
    optionally plot the results."""
    accepted_timestamp = "order_updated_timestamp"
    df[ORDER_TIMESTAMP] = pd.to_datetime(df[ORDER_TIMESTAMP])
    df[accepted_timestamp] = pd.to_datetime(df[accepted_timestamp])
    df.loc[:, "interval_index"] = (
        df[ORDER_TIMESTAMP].dt.hour * 60 + df[ORDER_TIMESTAMP].dt.minute
    ) // interval
    df.loc[:, "time_difference"] = (
        df[accepted_timestamp] - df[ORDER_TIMESTAMP]
    ).dt.total_seconds() / 60
    mean_time_difference = (
        df.groupby(["interval_index", df[ORDER_TIMESTAMP].dt.date])[
            "time_difference"
        ]
        .mean()
        .groupby("interval_index")
        .mean()
    )
    median_time_difference = (
        df.groupby(["interval_index", df[ORDER_TIMESTAMP].dt.date])[
            "time_difference"
        ]
        .median()
        .groupby("interval_index")
        .median()
    )
    std_time_difference = (
        df.groupby(["interval_index", df[ORDER_TIMESTAMP].dt.date])[
            "time_difference"
        ]
        .median()
        .groupby("interval_index")
        .std()
    )
    if plot:
        plot_statistics_with_interval(
            mean_time_difference,
            median_time_difference,
            std_time_difference,
            "Hour of the Day",
            "Time Difference in Order Acceptance (minutes)",
            f"Mean and Median Time Difference in Order Acceptance per {interval}-min on an Average Day",  # pylint: disable=line-too-long
        )
        return None
    return mean_time_difference, median_time_difference


def calculate_prep_time_per_interval(
    df: pd.DataFrame, interval: int, plot=False
):
    """Calculate preparation time per interval and optionally plot
    the results."""
    start_prep_time = "order_start_prepping_at_timestamp"
    end_prep_time = "order_prepare_for_timestamp"
    df[start_prep_time] = pd.to_datetime(df[start_prep_time])
    df[end_prep_time] = pd.to_datetime(df[end_prep_time])
    df.loc[:, "interval_index"] = (
        df[start_prep_time].dt.hour * 60 + df[start_prep_time].dt.minute
    ) // interval
    df.loc[:, "time_difference"] = (
        df[end_prep_time] - df[start_prep_time]
    ).dt.total_seconds() / 60
    mean_time_difference = (
        df.groupby(["interval_index", df[start_prep_time].dt.date])[
            "time_difference"
        ]
        .mean()
        .groupby("interval_index")
        .mean()
    )
    median_time_difference = (
        df.groupby(["interval_index", df[start_prep_time].dt.date])[
            "time_difference"
        ]
        .median()
        .groupby("interval_index")
        .median()
    )
    std_time_difference = (
        df.groupby(["interval_index", df[start_prep_time].dt.date])[
            "time_difference"
        ]
        .median()
        .groupby("interval_index")
        .std()
    )
    if plot:
        plot_statistics_with_interval(
            mean_time_difference,
            median_time_difference,
            std_time_difference,
            "Hour of the Day",
            "Prep Time Difference (minutes)",
            f"Mean and Median Prep Time per {interval}-min on an Average Day",
        )
        return None
    return mean_time_difference, median_time_difference


def calculate_items_sold(df: pd.DataFrame, plot=False):
    """Calculate items sold and optionally plot the results."""
    df = df[["order_id", "item_name", "item_quantity"]].copy()
    if plot:
        plot_items_sold(df)
        return None
    return df


def calculate_average_orders_by_day_of_week(df: pd.DataFrame, plot=False):
    """Calculate average orders by day of week and optionally plot the results."""
    df[ORDER_TIMESTAMP] = pd.to_datetime(df[ORDER_TIMESTAMP])
    df["day_of_week"] = df[ORDER_TIMESTAMP].dt.day_name()
    daily_orders = (
        df.groupby(["day_of_week", df[ORDER_TIMESTAMP].dt.date])["order_id"]
        .nunique()
        .reset_index(name="order_count")
    )
    mean_orders = daily_orders.groupby("day_of_week")["order_count"].mean()
    median_orders = daily_orders.groupby("day_of_week")["order_count"].median()
    if plot:
        plot_mean_and_median_statistics_by_weekday(
            mean_orders,
            median_orders,
            "Day of the Week",
            "Number of Orders",
            "Mean and Median Number of Orders by Day of the Week",
        )
        return None
    return mean_orders, median_orders


def calculate_average_revenue_by_day_of_week(df: pd.DataFrame, plot=False):
    """Calculate average revenue by day of week and
    optionally plot the results."""
    df[ORDER_TIMESTAMP] = pd.to_datetime(df[ORDER_TIMESTAMP])
    df = calculate_revenue(df)
    df["day_of_week"] = df[ORDER_TIMESTAMP].dt.day_name()
    daily_revenue = (
        df.groupby(["day_of_week", df[ORDER_TIMESTAMP].dt.date])
        .agg(daily_revenue=("order_value", "sum"))
        .reset_index()
    )
    mean_revenue_by_day = daily_revenue.groupby("day_of_week")[
        "daily_revenue"
    ].mean()
    median_revenue_by_day = daily_revenue.groupby("day_of_week")[
        "daily_revenue"
    ].median()
    if plot:
        plot_mean_and_median_statistics_by_weekday(
            mean_revenue_by_day,
            median_revenue_by_day,
            "Day of the Week",
            "Revenue",
            "Mean and Median Revenue by Day of the Week",
        )
        return None
    return mean_revenue_by_day, median_revenue_by_day


def calculate_revenue_by_day_period(
    df: pd.DataFrame, time_intervals: List[str]
) -> pd.DataFrame:
    """Calculate revenue by day period based on specified time intervals."""
    df[ORDER_TIMESTAMP] = pd.to_datetime(df[ORDER_TIMESTAMP])
    time_intervals = [
        pd.to_datetime(str(time)).time() for time in time_intervals
    ]
    df.loc[:, "order_value"] = (
        (df["item_fractional_price"] * df["item_quantity"])
        + (df["modifier_fractional_price"] * df["modifier_quantity"])
    ) / 100
    interval_labels = [
        f"{time_intervals[i]} to {time_intervals[i+1]}"
        for i in range(len(time_intervals) - 1)
    ]
    df.loc[:, "interval_label"] = pd.cut(
        df[ORDER_TIMESTAMP].dt.time,
        bins=time_intervals,
        labels=interval_labels,
    )
    return df.groupby("interval_label", observed=True)["order_value"].sum()


def calculate_profit_by_day_period(
    df: pd.DataFrame, time_intervals: List[str]
) -> pd.DataFrame:
    """Calculate profit by day period based on specified time intervals."""
    df[ORDER_TIMESTAMP] = pd.to_datetime(df[ORDER_TIMESTAMP])
    time_intervals = [
        pd.to_datetime(str(time)).time() for time in time_intervals
    ]
    df.loc[:, "order_value"] = (
        (df["item_fractional_price"] * df["item_quantity"])
        + (df["modifier_fractional_price"] * df["modifier_quantity"])
    ) / 100
    df.loc[:, "profit"] = df["order_value"] - (
        df["item_fractional_cost"] / 100
    )
    interval_labels = [
        f"{time_intervals[i]} to {time_intervals[i+1]}"
        for i in range(len(time_intervals) - 1)
    ]
    df.loc[:, "interval_label"] = pd.cut(
        df[ORDER_TIMESTAMP].dt.time,
        bins=time_intervals,
        labels=interval_labels,
    )
    return df.groupby("interval_label", observed=True)["profit"].sum()


def calculate_orders_by_day_period(
    df: pd.DataFrame, time_intervals: List[str]
) -> pd.DataFrame:
    """Count the number of orders by day period based on specified time intervals."""
    # Convert order timestamp column to datetime
    df[ORDER_TIMESTAMP] = pd.to_datetime(df[ORDER_TIMESTAMP])

    # Create a list of datetime.time objects from the provided time intervals
    time_intervals = [pd.to_datetime(time).time() for time in time_intervals]

    # Assign labels for each interval
    interval_labels = [
        f"{time_intervals[i]} to {time_intervals[i+1]}"
        for i in range(len(time_intervals) - 1)
    ]

    # Categorize each order into one of the intervals
    df["interval_label"] = pd.cut(
        df[ORDER_TIMESTAMP].dt.time,
        bins=time_intervals,
        labels=interval_labels,
        right=False,  # Use the left inclusive and right exclusive in interval
        include_lowest=True,  # Include the lowest value
    )

    # Count the number of orders in each interval
    order_counts = df.groupby("interval_label").size()

    return order_counts.reset_index(name="order_count")


def calculate_profits_over_periods(
    df: pd.DataFrame, time_intervals: List[str] = None, plot=False
) -> pd.DataFrame:
    """Calculate profits over specific periods and optionally plot the results."""
    df[ORDER_TIMESTAMP] = pd.to_datetime(df[ORDER_TIMESTAMP])
    df.sort_values(by=ORDER_TIMESTAMP, inplace=True)
    start_date = df[ORDER_TIMESTAMP].min()
    end_date = df[ORDER_TIMESTAMP].max()
    profit_results = pd.DataFrame()
    total_days = (end_date - start_date).days
    periods = total_days // 21  # 3 weeks * 7 days
    for period in range(
        periods + 1
    ):  # +1 to include the last period which may be < 3 weeks
        period_start = start_date + timedelta(weeks=3 * period)
        period_end = min(period_start + timedelta(weeks=3), end_date)
        period_df = df[
            (df[ORDER_TIMESTAMP] >= period_start)
            & (df[ORDER_TIMESTAMP] < period_end)
        ]
        unique_order_days = period_df[ORDER_TIMESTAMP].dt.date.nunique()
        if (
            unique_order_days >= 5
        ):  # At least 19 days with orders in a 21-day period
            period_profit = calculate_profit_by_day_period(
                period_df, time_intervals
            )
            period_profit["Period"] = period + 1
            profit_results = pd.concat(
                [
                    profit_results,
                    period_profit.to_frame(name=f"Period {period + 1}").T,
                ]
            )
        else:
            print(
                f"Period {period + 1} skipped due to insufficient order days ({unique_order_days} days)"  # pylint: disable=line-too-long
            )
    profit_results.reset_index(drop=True, inplace=True)
    if plot:
        plot_profits_over_time(profit_results)
        return None
    return profit_results


def generate_menu_matrix(df: pd.DataFrame, plot=False):
    """Generate a menu matrix analyzing item popularity and profitability."""
    df["item_revenue"] = (
        df["item_quantity"] * df["item_fractional_price"]
    ) / 100
    df["item_cost"] = (df["item_quantity"] * df["item_fractional_cost"]) / 100
    item_popularity = df.groupby("item_name")["item_quantity"].sum()
    item_revenue = df.groupby("item_name")["item_revenue"].sum()
    item_cost = df.groupby("item_name")["item_cost"].sum()
    item_profitability = (item_revenue - item_cost) / item_revenue
    popularity_threshold = item_popularity.quantile(0.5)  # Adjust as needed
    profitability_threshold = item_profitability.quantile(
        0.5
    )  # Adjust as needed

    def categorize_item(row):
        item_name = row["item_name"]
        popularity = item_popularity.get(item_name, 0)
        profitability = item_profitability.get(item_name, 0)
        if (
            popularity >= popularity_threshold
            and profitability >= profitability_threshold
        ):
            return "Star"
        if (
            popularity < popularity_threshold
            and profitability >= profitability_threshold
        ):
            return "Puzzle"
        if (
            popularity >= popularity_threshold
            and profitability < profitability_threshold
        ):
            return "Cash Cow"
        return "Dud"

    df["category"] = df.apply(categorize_item, axis=1)
    df["item_popularity"] = df["item_name"].map(item_popularity)
    df["item_profitability"] = df["item_name"].map(item_profitability)
    if plot:
        plot_menu_matrix(df)
        return None
    return df
