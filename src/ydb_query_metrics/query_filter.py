#!/usr/bin/env python3
"""
Query Filter Module for Query Metrics Processor

Contains functions for filtering queries based on various criteria.
"""

import re
import pandas as pd
from typing import List


def filter_queries(df: pd.DataFrame, like_filters: List[str], not_like_filters: List[str], regex_filters: List[str] = None) -> pd.DataFrame:
    """
    Filter queries based on 'like', 'not like', and regex patterns.
    
    Args:
        df: DataFrame of query data
        like_filters: List of patterns to include (substring match)
        not_like_filters: List of patterns to exclude (substring match)
        regex_filters: List of regular expressions to match (can be None)
        
    Returns:
        Filtered DataFrame
    """
    # Start with all rows
    mask = pd.Series([True] * len(df), index=df.index)
    
    # Apply 'like' filters (AND logic)
    for pattern in like_filters:
        pattern_mask = df['QueryText'].str.contains(pattern, case=False, regex=False)
        mask = mask & pattern_mask
    
    # Apply 'not like' filters (AND logic)
    for pattern in not_like_filters:
        pattern_mask = ~df['QueryText'].str.contains(pattern, case=False, regex=False)
        mask = mask & pattern_mask
    
    # Apply regex filters (AND logic)
    if regex_filters:
        for pattern in regex_filters:
            try:
                pattern_mask = df['QueryText'].str.contains(pattern, case=False, regex=True)
                mask = mask & pattern_mask
            except re.error as e:
                import click  # Import here to avoid circular imports
                click.echo(f"Warning: Invalid regex pattern '{pattern}': {e}", err=True)
    
    return df[mask]