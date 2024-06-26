"""
This module contains tests for various analysis utilities used in the dynamic
pricing application. It verifies the correctness of functions calculating
different metrics like revenue, profit, and preparation times.
"""

import pandas as pd
import pytest

from dynamic_pricing.analysis.metrics import (
    calculate_average_orders_by_day_of_week,
    calculate_average_orders_per_interval,
    calculate_average_revenue_by_day_of_week,
    calculate_average_revenue_per_interval,
    calculate_prep_time_per_interval,
    calculate_profit_by_day_period,
    calculate_revenue,
    calculate_revenue_by_day_period,
    calculate_time_difference_in_order_acceptance_per_interval,
    calculate_orders_by_day_period,
)

data = {
    "order_id": [1, 2, 3, 4, 4, 5],
    "item_quantity": [1, 1, 1, 1, 1, 1],
    "item_fractional_price": [100, 200, 300, 400, 500, 600],
    "modifier_fractional_price": [100, 0, 100, 0, 0, 0],
    "modifier_quantity": [1, 0, 1, 0, 0, 0],
    "item_fractional_cost": [10, 20, 30, 40, 50, 60],
    "item_name": ["Burger", "Pizza", "Salad", "Fries", "Soda", "Ice Cream"],
    "order_placed_timestamp": [
        "2024-01-01 12:30:00",
        "2024-01-02 14:45:00",
        "2024-01-08 11:05:00",
        "2024-01-09 16:20:00",
        "2024-01-09 16:20:00",
        "2024-01-09 09:15:00",
    ],
    "order_updated_timestamp": [
        "2024-01-01 12:35:00",
        "2024-01-02 14:50:00",
        "2024-01-08 11:10:00",
        "2024-01-09 16:25:00",
        "2024-01-09 16:25:00",
        "2024-01-09 09:20:00",
    ],
    "order_start_prepping_at_timestamp": [
        "2024-01-01 12:45:00",
        "2024-01-02 15:00:00",
        "2024-01-08 11:20:00",
        "2024-01-09 16:35:00",
        "2024-01-09 16:35:00",
        "2024-01-09 09:30:00",
    ],
    "order_prepare_for_timestamp": [
        "2024-01-01 13:00:00",
        "2024-01-02 15:15:00",
        "2024-01-08 11:35:00",
        "2024-01-09 16:50:00",
        "2024-01-09 16:50:00",
        "2024-01-09 09:45:00",
    ],
}


@pytest.fixture
def sample_df():
    """Fixture to provide a DataFrame setup for testing."""
    return pd.DataFrame(data)


@pytest.fixture
def sample_time_intervals():
    """Fixture to provide a list of time intervals for testing revenue
    calculations."""
    return ["00:00", "14:00", "18:45", "23:59:59"]


def test_calculate_revenue(sample_df):  # pylint: disable=W0621
    """Test to verify the revenue calculation from the given DataFrame."""
    result_df = calculate_revenue(sample_df)
    assert result_df["revenue"].to_list() == [2.0, 2.0, 4.0, 9.0, 9.0, 6.0]


def test_calculate_average_orders_per_interval(
    sample_df,
):  # pylint: disable=W0621
    """Test to verify the average and median orders per time interval."""
    interval = 30  # Example interval of 30 minutes
    mean_orders, median_orders = calculate_average_orders_per_interval(
        sample_df, interval, plot=False
    )
    assert mean_orders.to_list() == [1.0, 1.0, 1.0, 1.0, 2.0]
    assert median_orders.to_list() == [1.0, 1.0, 1.0, 1.0, 2.0]


def test_calculate_average_revenue_per_interval(
    sample_df,
):  # pylint: disable=W0621
    """Test to verify the average and median revenue per time interval."""
    interval = 30
    mean_revenue, median_revenue = calculate_average_revenue_per_interval(
        sample_df, interval, plot=False
    )
    assert mean_revenue.to_list() == [6.0, 4.0, 2.0, 2.0, 9.0]
    assert median_revenue.to_list() == [6.0, 4.0, 2.0, 2.0, 9.0]


def test_calculate_average_orders_by_day_of_week(
    sample_df,
):  # pylint: disable=W0621
    """Test to verify the average and median orders by day of the week."""
    mean_orders, median_orders = calculate_average_orders_by_day_of_week(
        sample_df, plot=False
    )
    assert mean_orders.to_list() == [1.0, 1.5]
    assert median_orders.to_list() == [1.0, 1.5]


def test_calculate_average_revenue_by_day_of_week(
    sample_df,
):  # pylint: disable=W0621
    """Test to verify the average and median revenue by day of the week."""
    mean_revenue, median_revenue = calculate_average_revenue_by_day_of_week(
        sample_df, plot=False
    )
    assert mean_revenue.to_list() == [3.0, 8.5]
    assert median_revenue.to_list() == [3.0, 8.5]


def test_calculate_time_difference_in_order_acceptance_per_interval(  # pylint: disable=W0621
    sample_df,
):
    """Test to verify the mean and median time differences in order acceptance
    per interval."""
    interval = 30
    mean_order_acceptance_time, median_order_acceptance_time = (
        calculate_time_difference_in_order_acceptance_per_interval(
            sample_df, interval, plot=False
        )
    )
    assert mean_order_acceptance_time.to_list() == [5.0, 5.0, 5.0, 5.0, 5.0]
    assert median_order_acceptance_time.to_list() == [5.0, 5.0, 5.0, 5.0, 5.0]


def test_calculate_prep_time_per_interval(sample_df):  # pylint: disable=W0621
    """Test to verify the mean and median preparation times
    per time interval."""
    interval = 30
    mean_prep_time, median_prep_time = calculate_prep_time_per_interval(
        sample_df, interval, plot=False
    )
    assert mean_prep_time.to_list() == [15.0, 15.0, 15.0, 15.0, 15.0]
    assert median_prep_time.to_list() == [15.0, 15.0, 15.0, 15.0, 15.0]


def test_calculate_revenue_by_day_period(
    sample_df, sample_time_intervals
):  # pylint: disable=W0621
    """Test to verify the revenue calculation by different time periods of the
    day."""
    result_series = calculate_revenue_by_day_period(
        sample_df, sample_time_intervals
    )
    assert result_series.to_list() == [12.0, 11.0, 0.0]


def test_calculate_profit_by_day_period(
    sample_df, sample_time_intervals
):  # pylint: disable=W0621
    """Test to verify the profit calculation by different time periods of the
    day."""
    result_series = calculate_profit_by_day_period(
        sample_df, sample_time_intervals
    )
    assert result_series.to_list() == [11.0, 9.9, 0.0]


def test_calculate_ordes_by_day_period(
    sample_df, sample_time_intervals
):  # pylint: disable=W0621
    """Test to verify the orders calculation by different time periods of the
    day."""
    result = calculate_orders_by_day_period(sample_df, sample_time_intervals)
    assert result["order_count"].to_list() == [3, 3, 0]
