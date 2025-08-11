import pytest
import pandas as pd
import re
from ydb_query_metrics.query_filter import filter_queries


class TestQueryFilter:
    """Tests for the query_filter module."""

    def test_filter_queries_no_filters(self, query_metrics_df):
        """Test filtering with no filters applied."""
        filtered_df = filter_queries(query_metrics_df, [], [])
        
        # Should return all rows
        assert len(filtered_df) == len(query_metrics_df)
        assert filtered_df.equals(query_metrics_df)

    def test_filter_queries_like(self, query_metrics_df):
        """Test filtering with 'like' filter."""
        # Filter for queries containing 'table_alpha'
        filtered_df = filter_queries(query_metrics_df, ['table_alpha'], [])
        
        # Should return only rows with 'table_alpha' in QueryText
        assert len(filtered_df) == 1
        assert 'table_alpha' in filtered_df['QueryText'].iloc[0]

    def test_filter_queries_not_like(self, query_metrics_df):
        """Test filtering with 'not like' filter."""
        # Filter out queries containing 'table_alpha'
        filtered_df = filter_queries(query_metrics_df, [], ['table_alpha'])
        
        # Should return rows without 'table_alpha' in QueryText
        assert len(filtered_df) == 2
        for query_text in filtered_df['QueryText']:
            assert 'table_alpha' not in query_text

    def test_filter_queries_like_and_not_like(self, query_metrics_df):
        """Test filtering with both 'like' and 'not like' filters."""
        # Filter for queries containing 'SELECT' but not 'table_alpha'
        filtered_df = filter_queries(query_metrics_df, ['SELECT'], ['table_alpha'])
        
        # Should return rows with 'SELECT' but without 'table_alpha' in QueryText
        assert len(filtered_df) == 2
        for query_text in filtered_df['QueryText']:
            assert 'SELECT' in query_text
            assert 'table_alpha' not in query_text

    def test_filter_queries_multiple_like(self, query_metrics_df):
        """Test filtering with multiple 'like' filters (AND logic)."""
        # Filter for queries containing both 'SELECT' and 'WHERE'
        filtered_df = filter_queries(query_metrics_df, ['SELECT', 'WHERE'], [])
        
        # Should return rows with both 'SELECT' and 'WHERE' in QueryText
        assert len(filtered_df) == 2  # First two queries have both SELECT and WHERE
        for query_text in filtered_df['QueryText']:
            assert 'SELECT' in query_text
            assert 'WHERE' in query_text

    def test_filter_queries_multiple_not_like(self, query_metrics_df):
        """Test filtering with multiple 'not like' filters (AND logic)."""
        # Filter out queries containing either 'table_alpha' or 'table_beta'
        filtered_df = filter_queries(query_metrics_df, [], ['table_alpha', 'table_beta'])
        
        # Should return rows without 'table_alpha' or 'table_beta' in QueryText
        assert len(filtered_df) == 1  # Only the third query doesn't have either
        for query_text in filtered_df['QueryText']:
            assert 'table_alpha' not in query_text
            assert 'table_beta' not in query_text

    def test_filter_queries_regex(self, query_metrics_df):
        """Test filtering with regex pattern."""
        # Filter for queries matching the regex pattern 'table_[a-z]+'
        filtered_df = filter_queries(query_metrics_df, [], [], ['table_[a-z]+'])
        
        # Should return all rows as all queries contain a pattern like 'table_alpha'
        assert len(filtered_df) == 3
        
        # Try a more specific regex
        filtered_df = filter_queries(query_metrics_df, [], [], ['table_a[a-z]+'])
        
        # Should return only the first query with 'table_alpha'
        assert len(filtered_df) == 1
        assert 'table_alpha' in filtered_df['QueryText'].iloc[0]

    def test_filter_queries_invalid_regex(self, query_metrics_df):
        """Test filtering with invalid regex pattern."""
        # This should raise an exception
        with pytest.raises(re.error):
            filtered_df = filter_queries(query_metrics_df, [], [], ['[invalid regex'])
        
    def test_filter_queries_case_insensitive(self, query_metrics_df):
        """Test that filtering is case-insensitive."""
        # Filter for queries containing 'select' (lowercase)
        filtered_df = filter_queries(query_metrics_df, ['select'], [])
        
        # Should return all rows as all queries contain 'SELECT' (uppercase)
        assert len(filtered_df) == 3
        
        # Filter for queries containing 'TABLE_ALPHA' (uppercase)
        filtered_df = filter_queries(query_metrics_df, ['TABLE_ALPHA'], [])
        
        # Should return the first row as it contains 'table_alpha' (lowercase)
        assert len(filtered_df) == 1
        assert 'table_alpha' in filtered_df['QueryText'].iloc[0].lower()