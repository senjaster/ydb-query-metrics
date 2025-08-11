import os
import pytest
import pandas as pd
from ydb_query_metrics.file_format import (
    detect_file_format, 
    transform_top_queries_to_query_metrics,
    detect_encoding,
    has_headers,
    detect_and_load_file,
    load_tsv_file
)


class TestFileFormat:
    """Tests for the file_format module."""

    def test_detect_file_format_query_metrics(self, query_metrics_df):
        """Test detecting query_metrics format."""
        format_type = detect_file_format(query_metrics_df)
        assert format_type == 'query_metrics'

    def test_detect_file_format_top_queries(self, top_queries_df):
        """Test detecting top_queries format."""
        format_type = detect_file_format(top_queries_df)
        assert format_type == 'top_queries'

    def test_transform_top_queries_to_query_metrics(self, top_queries_df):
        """Test transforming top_queries format to query_metrics format."""
        transformed_df = transform_top_queries_to_query_metrics(top_queries_df)
        
        # Check that the transformed DataFrame has the expected columns
        expected_columns = [
            'IntervalEnd', 'MinCPUTime', 'MaxCPUTime', 'SumCPUTime',
            'MinDuration', 'MaxDuration', 'SumDuration',
            'MinReadRows', 'MaxReadRows', 'SumReadRows',
            'MinReadBytes', 'MaxReadBytes', 'SumReadBytes',
            'MinUpdateRows', 'MaxUpdateRows', 'SumUpdateRows',
            'MinUpdateBytes', 'MaxUpdateBytes', 'SumUpdateBytes',
            'QueryText', 'Rank', 'Count'
        ]
        for col in expected_columns:
            assert col in transformed_df.columns
        
        # Check that values are correctly transformed
        # Convert to float for comparison since the transformed values are floats
        assert transformed_df['MinCPUTime'].astype(float).equals(top_queries_df['CPUTime'].astype(float))
        assert transformed_df['MaxCPUTime'].astype(float).equals(top_queries_df['CPUTime'].astype(float))
        assert transformed_df['SumCPUTime'].astype(float).equals(top_queries_df['CPUTime'].astype(float))
        
        assert transformed_df['MinDuration'].astype(float).equals(top_queries_df['Duration'].astype(float))
        assert transformed_df['MaxDuration'].astype(float).equals(top_queries_df['Duration'].astype(float))
        assert transformed_df['SumDuration'].astype(float).equals(top_queries_df['Duration'].astype(float))
        
        # Check that Count is set to 1.0 for each row
        assert all(transformed_df['Count'] == 1.0)

    def test_has_headers_with_headers(self, query_metrics_df):
        """Test has_headers with a DataFrame that has headers."""
        assert has_headers(query_metrics_df) is True

    def test_has_headers_without_headers(self):
        """Test has_headers with a DataFrame that doesn't have headers."""
        # Create a DataFrame without headers
        df = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
        assert has_headers(df) is False

    def test_detect_encoding(self, test_data_dir):
        """Test detect_encoding function."""
        # This is a basic test since we can't easily create files with different encodings
        file_path = os.path.join(test_data_dir, 'query_metrics_sample.tsv')
        encoding = detect_encoding(file_path)
        assert encoding in ['utf-8', 'utf-8-sig', 'utf-16le', 'utf-16be']

    def test_load_tsv_file_query_metrics(self, test_data_dir):
        """Test loading a query_metrics TSV file."""
        file_path = os.path.join(test_data_dir, 'query_metrics_sample.tsv')
        df = load_tsv_file(file_path)
        
        # Check that the DataFrame has the expected columns and data
        assert 'QueryText' in df.columns
        assert 'MinDuration' in df.columns
        assert 'MaxDuration' in df.columns
        assert len(df) > 0
        
        # Check that the first query contains expected text
        assert 'table_alpha' in df['QueryText'].iloc[0]

    def test_load_tsv_file_top_queries(self, test_data_dir):
        """Test loading a top_queries TSV file."""
        file_path = os.path.join(test_data_dir, 'top_queries_sample.tsv')
        df = load_tsv_file(file_path)
        
        # The function should transform top_queries to query_metrics format
        assert 'MinDuration' in df.columns
        assert 'MaxDuration' in df.columns
        assert 'SumDuration' in df.columns
        assert len(df) > 0
        
        # Check that the first query contains expected text
        assert 'table_delta' in df['QueryText'].iloc[0]

    def test_load_tsv_file_with_format_hint(self, test_data_dir):
        """Test loading a TSV file with a format hint."""
        file_path = os.path.join(test_data_dir, 'query_metrics_sample.tsv')
        
        # Load with explicit format hint
        df = load_tsv_file(file_path, format_hint='query_metrics')
        
        assert 'QueryText' in df.columns
        assert 'MinDuration' in df.columns
        assert len(df) > 0