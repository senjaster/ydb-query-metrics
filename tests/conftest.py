import os
import pytest
import pandas as pd
from datetime import datetime

# Constants for test data
TEST_DATE = datetime(2025, 1, 1)
TEST_DATE_STR = "2025-01-01"


@pytest.fixture
def test_data_dir():
    """Return the path to the test data directory."""
    return os.path.join(os.path.dirname(__file__), "fixtures")


@pytest.fixture
def query_metrics_df():
    """Create a sample DataFrame in query_metrics format."""
    data = {
        'Count': [1.0, 2.0, 3.0],
        'IntervalEnd': [TEST_DATE_STR, TEST_DATE_STR, TEST_DATE_STR],
        'MinDuration': [100000, 200000, 300000],
        'MaxDuration': [500000, 600000, 700000],
        'SumDuration': [1000000, 2000000, 3000000],
        'MinCPUTime': [10000, 20000, 30000],
        'MaxCPUTime': [50000, 60000, 70000],
        'SumCPUTime': [100000, 200000, 300000],
        'MinReadRows': [10, 20, 30],
        'MaxReadRows': [50, 60, 70],
        'SumReadRows': [100, 200, 300],
        'MinReadBytes': [1000, 2000, 3000],
        'MaxReadBytes': [5000, 6000, 7000],
        'SumReadBytes': [10000, 20000, 30000],
        'MinUpdateRows': [1, 2, 3],
        'MaxUpdateRows': [5, 6, 7],
        'SumUpdateRows': [10, 20, 30],
        'MinUpdateBytes': [100, 200, 300],
        'MaxUpdateBytes': [500, 600, 700],
        'SumUpdateBytes': [1000, 2000, 3000],
        'QueryText': [
            'SELECT * FROM table_alpha WHERE id = 123',
            'SELECT name, value FROM table_beta WHERE status = "active"',
            'SELECT COUNT(*) FROM table_gamma GROUP BY category'
        ],
        'Rank': [1, 2, 3]
    }
    return pd.DataFrame(data)


@pytest.fixture
def top_queries_df():
    """Create a sample DataFrame in top_queries format."""
    data = {
        'CPUTime': [10000, 20000, 30000],
        'Duration': [100000, 200000, 300000],
        'EndTime': [TEST_DATE_STR, TEST_DATE_STR, TEST_DATE_STR],
        'IntervalEnd': [TEST_DATE_STR, TEST_DATE_STR, TEST_DATE_STR],
        'ReadRows': [10, 20, 30],
        'ReadBytes': [1000, 2000, 3000],
        'UpdateRows': [1, 2, 3],
        'UpdateBytes': [100, 200, 300],
        'QueryText': [
            'SELECT * FROM table_delta WHERE id = 456',
            'SELECT name, value FROM table_epsilon WHERE status = "pending"',
            'SELECT COUNT(*) FROM table_zeta GROUP BY region'
        ],
        'Rank': [1, 2, 3]
    }
    return pd.DataFrame(data)


@pytest.fixture
def query_statistics_sample(query_metrics_df):
    """Create sample QueryStatistics objects from the test DataFrame."""
    from ydb_query_metrics.query_statistics import calculate_statistics
    return calculate_statistics(query_metrics_df)


@pytest.fixture
def create_tsv_file(test_data_dir):
    """Create a TSV file from a DataFrame."""
    def _create_file(df, filename):
        filepath = os.path.join(test_data_dir, filename)
        df.to_csv(filepath, sep='\t', index=False)
        return filepath
    return _create_file