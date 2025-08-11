import pytest
import pandas as pd
from ydb_query_metrics.query_statistics import (
    MetricStats,
    QueryStatistics,
    calculate_statistics
)


class TestMetricStats:
    """Tests for the MetricStats class."""

    def test_create_for_metric(self):
        """Test creating a MetricStats instance for a specific metric."""
        # Create for Duration metric
        duration_stats = MetricStats.create_for_metric('Duration', 1_000_000)
        
        assert duration_stats.min_column == 'MinDuration'
        assert duration_stats.max_column == 'MaxDuration'
        assert duration_stats.sum_column == 'SumDuration'
        assert duration_stats.scale == 1_000_000
        assert duration_stats.metric_name == 'Duration (s)'
        
        # Create for ReadRows metric
        read_rows_stats = MetricStats.create_for_metric('ReadRows')
        
        assert read_rows_stats.min_column == 'MinReadRows'
        assert read_rows_stats.max_column == 'MaxReadRows'
        assert read_rows_stats.sum_column == 'SumReadRows'
        assert read_rows_stats.scale == 1.0
        assert read_rows_stats.metric_name == 'ReadRows'

    def test_avg_property(self):
        """Test the avg property calculation."""
        stats = MetricStats(
            min_column='MinTest',
            max_column='MaxTest',
            sum_column='SumTest',
            scale=10.0,
            metric_name='Test'
        )
        
        # Set values
        stats.sum = 100.0
        stats._total_count = 5.0
        
        # Calculate average: 100 / (5 * 10) = 2.0
        assert stats.avg == 2.0
        
        # Test with zero count
        stats._total_count = 0.0
        assert stats.avg == 0.0

    def test_update(self):
        """Test updating statistics with values from a DataFrame row."""
        stats = MetricStats(
            min_column='MinTest',
            max_column='MaxTest',
            sum_column='SumTest',
            scale=10.0,
            metric_name='Test'
        )
        
        # Create a test row
        row = pd.Series({
            'Count': 2.0,
            'MinTest': 50.0,
            'MaxTest': 100.0,
            'SumTest': 150.0
        })
        
        # Initial values
        stats.min = float('inf')
        stats.max = 0.0
        stats.sum = 0.0
        
        # Update with row
        stats.update(row)
        
        # Check updated values
        assert stats._total_count == 2.0
        assert stats.min == 5.0  # 50 / 10
        assert stats.max == 10.0  # 100 / 10
        assert stats.sum == 150.0  # Sum is not scaled for storage
        
        # Update with another row
        row2 = pd.Series({
            'Count': 3.0,
            'MinTest': 30.0,
            'MaxTest': 200.0,
            'SumTest': 250.0
        })
        
        stats.update(row2)
        
        # Check updated values
        assert stats._total_count == 5.0  # 2 + 3
        assert stats.min == 3.0  # min(5.0, 30/10)
        assert stats.max == 20.0  # max(10.0, 200/10)
        assert stats.sum == 400.0  # 150 + 250


class TestQueryStatistics:
    """Tests for the QueryStatistics class."""

    def test_initialization(self):
        """Test initializing a QueryStatistics instance."""
        query_text = "SELECT * FROM test_table"
        stats = QueryStatistics(query_text)
        
        assert stats.query_text == query_text
        assert stats.row_count == 0
        assert stats.total_count == 0.0
        
        # Check that metric stats are initialized
        assert isinstance(stats.duration, MetricStats)
        assert isinstance(stats.cpu_time, MetricStats)
        assert isinstance(stats.read_rows, MetricStats)
        assert isinstance(stats.read_bytes, MetricStats)
        assert isinstance(stats.update_rows, MetricStats)
        assert isinstance(stats.update_bytes, MetricStats)

    def test_rows_per_second(self):
        """Test rows_per_second property calculation."""
        stats = QueryStatistics("SELECT * FROM test_table")
        
        # Set values for read_rows and duration
        stats.read_rows.sum = 1000.0
        stats.read_rows._total_count = 10.0
        stats.duration.sum = 5_000_000.0  # 5 seconds in nanoseconds
        stats.duration._total_count = 10.0
        
        # Calculate rows per second: (1000/10) / (5_000_000/10/1_000_000) = 100 / 0.5 = 200
        assert stats.rows_per_second == 200.0
        
        # Test with zero duration
        stats.duration.sum = 0.0
        assert stats.rows_per_second == 0.0

    def test_bytes_per_row(self):
        """Test bytes_per_row property calculation."""
        stats = QueryStatistics("SELECT * FROM test_table")
        
        # Set values for read_bytes and read_rows
        stats.read_bytes.sum = 10000.0
        stats.read_bytes._total_count = 10.0
        stats.read_rows.sum = 500.0
        stats.read_rows._total_count = 10.0
        
        # Calculate bytes per row: (10000/10) / (500/10) = 1000 / 50 = 20
        assert stats.bytes_per_row == 20.0
        
        # Test with zero read_rows
        stats.read_rows.sum = 0.0
        assert stats.bytes_per_row == 0.0

    def test_update_from_row(self):
        """Test updating statistics from a DataFrame row."""
        stats = QueryStatistics("SELECT * FROM test_table")
        
        # Create a test row
        row = pd.Series({
            'Count': 2.0,
            'MinDuration': 100000.0,
            'MaxDuration': 500000.0,
            'SumDuration': 600000.0,
            'MinCPUTime': 10000.0,
            'MaxCPUTime': 50000.0,
            'SumCPUTime': 60000.0,
            'MinReadRows': 10.0,
            'MaxReadRows': 50.0,
            'SumReadRows': 60.0,
            'MinReadBytes': 1000.0,
            'MaxReadBytes': 5000.0,
            'SumReadBytes': 6000.0,
            'MinUpdateRows': 1.0,
            'MaxUpdateRows': 5.0,
            'SumUpdateRows': 6.0,
            'MinUpdateBytes': 100.0,
            'MaxUpdateBytes': 500.0,
            'SumUpdateBytes': 600.0
        })
        
        # Update with row
        stats.update_from_row(row)
        
        # Check updated values
        assert stats.row_count == 1
        assert stats.total_count == 2.0
        
        # Check that metric stats were updated
        assert stats.duration._total_count == 2.0
        assert stats.cpu_time._total_count == 2.0
        assert stats.read_rows._total_count == 2.0
        assert stats.read_bytes._total_count == 2.0
        assert stats.update_rows._total_count == 2.0
        assert stats.update_bytes._total_count == 2.0


class TestCalculateStatistics:
    """Tests for the calculate_statistics function."""

    def test_calculate_statistics(self, query_metrics_df):
        """Test calculating statistics from a DataFrame."""
        stats = calculate_statistics(query_metrics_df)
        
        # Check that we have the right number of unique queries
        assert len(stats) == 3
        
        # Check that each query has statistics
        for query_text in query_metrics_df['QueryText'].unique():
            assert query_text in stats
            assert isinstance(stats[query_text], QueryStatistics)
            
        # Check specific values for the first query
        first_query = query_metrics_df['QueryText'].iloc[0]
        first_stats = stats[first_query]
        
        assert first_stats.row_count == 1
        assert first_stats.total_count == query_metrics_df['Count'].iloc[0]
        
        # Check that min/max/sum values were correctly transferred
        assert first_stats.duration.min == query_metrics_df['MinDuration'].iloc[0] / 1_000_000
        assert first_stats.duration.max == query_metrics_df['MaxDuration'].iloc[0] / 1_000_000
        assert first_stats.duration.sum == query_metrics_df['SumDuration'].iloc[0]

    def test_calculate_statistics_empty_df(self):
        """Test calculating statistics from an empty DataFrame."""
        empty_df = pd.DataFrame(columns=[
            'Count', 'QueryText', 'MinDuration', 'MaxDuration', 'SumDuration'
        ])
        
        stats = calculate_statistics(empty_df)
        
        # Should return an empty dictionary
        assert len(stats) == 0
        assert isinstance(stats, dict)

    def test_calculate_statistics_skip_empty_queries(self):
        """Test that empty or NaN QueryText values are skipped."""
        df = pd.DataFrame({
            'Count': [1.0, 2.0, 3.0],
            'QueryText': ['SELECT * FROM table1', '', None],
            'MinDuration': [100000, 200000, 300000],
            'MaxDuration': [500000, 600000, 700000],
            'SumDuration': [1000000, 2000000, 3000000]
        })
        
        stats = calculate_statistics(df)
        
        # Should only include the first query
        assert len(stats) == 1
        assert 'SELECT * FROM table1' in stats