#!/usr/bin/env python3
"""
Query Filter Module for Query Metrics Processor

Contains classes and functions for filtering queries based on various criteria.
"""

import re
import pandas as pd
from typing import List, Callable, Optional


class QueryFilterBuilder:
    """
    Builder class for creating query filters.
    
    This class implements the Builder pattern to iteratively construct
    a filter function that can be applied to a DataFrame of query data.
    Each method adds a specific type of filter to the chain.
    """
    
    def __init__(self, column: str = 'QueryText'):
        """
        Initialize a new QueryFilterBuilder.
        
        Args:
            column: The DataFrame column to apply filters to (default: 'QueryText')
        """
        self.column = column
        self._filters = []
    
    def with_like_filters(self, patterns: List[str]) -> 'QueryFilterBuilder':
        """
        Add substring inclusion filters (like).
        
        Args:
            patterns: List of patterns to include (substring match)
            
        Returns:
            Self for method chaining
        """
        if not patterns:
            return self
            
        for pattern in patterns:
            self._filters.append(
                lambda df, p=pattern: df[self.column].str.contains(p, case=False, regex=False)
            )
        return self
    
    def with_not_like_filters(self, patterns: List[str]) -> 'QueryFilterBuilder':
        """
        Add substring exclusion filters (not like).
        
        Args:
            patterns: List of patterns to exclude (substring match)
            
        Returns:
            Self for method chaining
        """
        if not patterns:
            return self
            
        for pattern in patterns:
            self._filters.append(
                lambda df, p=pattern: ~df[self.column].str.contains(p, case=False, regex=False)
            )
        return self
    
    def with_regex_filters(self, patterns: Optional[List[str]]) -> 'QueryFilterBuilder':
        """
        Add regular expression filters.
        
        Args:
            patterns: List of regular expressions to match (can be None)
            
        Returns:
            Self for method chaining
        """
        if not patterns:
            return self
            
        for pattern in patterns:
            self._filters.append(
                lambda df, p=pattern: ~df[self.column].str.contains(p, case=False, regex=True)
            )
        return self
    
    def build(self) -> Callable[[pd.DataFrame], pd.DataFrame]:
        """
        Build the filter function.
        
        Returns:
            A function that takes a DataFrame and returns a filtered DataFrame
        """
        def filter_function(df: pd.DataFrame) -> pd.DataFrame:
            if not self._filters:
                return df
                
            # Start with all rows
            mask = pd.Series([True] * len(df), index=df.index)
            
            # Apply all filters with AND logic
            for filter_func in self._filters:
                mask = mask & filter_func(df)
                
            return df[mask]
            
        return filter_function


def filter_queries(df: pd.DataFrame, like_filters: List[str], not_like_filters: List[str], regex_filters: List[str] = None) -> pd.DataFrame:
    """
    Filter queries based on 'like', 'not like', and regex patterns.
    
    This function maintains backward compatibility with the original API
    but uses the QueryFilterBuilder internally.
    
    Args:
        df: DataFrame of query data
        like_filters: List of patterns to include (substring match)
        not_like_filters: List of patterns to exclude (substring match)
        regex_filters: List of regular expressions to match (can be None)
        
    Returns:
        Filtered DataFrame
    """
    filter_func = (QueryFilterBuilder()
                  .with_like_filters(like_filters)
                  .with_not_like_filters(not_like_filters)
                  .with_regex_filters(regex_filters)
                  .build())
    
    return filter_func(df)