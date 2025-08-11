#!/usr/bin/env python3
"""
File Format Module for Query Metrics Processor

Contains functions for detecting file formats, encoding, and loading TSV files.
"""

import os
import pandas as pd
from typing import List, Dict, Tuple, Optional, Set

# Define default column names for different formats
QUERY_METRICS_COLUMNS = [
    'Count', 'IntervalEnd', 'MaxCPUTime', 'MaxDeleteRows', 'MaxDuration',
    'MaxReadBytes', 'MaxReadRows', 'MaxRequestUnits', 'MaxUpdateBytes',
    'MaxUpdateRows', 'MinCPUTime', 'MinDeleteRows', 'MinDuration',
    'MinReadBytes', 'MinReadRows', 'MinRequestUnits', 'MinUpdateBytes',
    'MinUpdateRows', 'QueryText', 'Rank', 'SumCPUTime', 'SumDeleteRows',
    'SumDuration', 'SumReadBytes', 'SumReadRows', 'SumRequestUnits',
    'SumUpdateBytes', 'SumUpdateRows'
]

TOP_QUERIES_COLUMNS = [
    'CPUTime', 'CompileCPUTime', 'CompileDuration', 'ComputeNodesCount',
    'DeleteBytes', 'DeleteRows', 'Duration', 'EndTime', 'FromQueryCache',
    'IntervalEnd', 'MaxComputeCPUTime', 'MaxShardCPUTime', 'MinComputeCPUTime',
    'MinShardCPUTime', 'ParametersSize', 'Partitions', 'ProcessCPUTime',
    'QueryText', 'Rank', 'ReadBytes', 'ReadRows', 'RequestUnits',
    'ShardCount', 'SumComputeCPUTime', 'SumShardCPUTime', 'Type',
    'UpdateBytes', 'UpdateRows', 'UserSID'
]


def detect_file_format(df: pd.DataFrame) -> str:
    """
    Detect the format of the TSV file based on file name and data structure.
    
    Args:
        df: DataFrame with the TSV data
        
    Returns:
        'query_metrics' or 'top_queries'
    """

    # If file has headers, check column names
    if len(df.columns) > 0 and isinstance(df.columns[0], str):
        # Check for columns that are unique to each format
        if 'MinDuration' in df.columns and 'MaxDuration' in df.columns:
            return 'query_metrics'
        elif 'CPUTime' in df.columns and 'Duration' in df.columns:
            return 'top_queries'
        
    first_row_values = [str(val).strip() for val in df.iloc[0].values if not pd.isna(val)]
    
    # Check for columns that are unique to each format
    if 'MinDuration' in first_row_values and 'MaxDuration' in first_row_values:
        return 'query_metrics'
    elif 'CPUTime' in first_row_values and 'Duration' in first_row_values:
        return 'top_queries'

    # If no headers or can't determine from column names, check data structure
    # Count columns that might indicate the format
    col_count = len(df.columns)
    
    # If there are no headers use column counts
    if col_count == 29:
        return 'top_queries'
    elif col_count == 28:
        return 'query_metrics'
    
    raise ValueError("Unable to detect file format")


def transform_top_queries_to_query_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform data from top_queries format to query_metrics format.
    
    Args:
        df: DataFrame with top_queries data
        
    Returns:
        DataFrame with query_metrics format
    """
    # Create a new DataFrame with query_metrics columns
    result_df = pd.DataFrame()
    
    # Skip header row if it exists
    if len(df) > 0 and 'CPUTime' in df.columns and isinstance(df['CPUTime'].iloc[0], str) and df['CPUTime'].iloc[0] == 'CPUTime':
        df = df.iloc[1:].reset_index(drop=True)
    
    # Ensure numeric columns are converted to appropriate types
    numeric_columns = ['CPUTime', 'Duration', 'ReadRows', 'ReadBytes',
                       'UpdateRows', 'UpdateBytes', 'DeleteRows', 'RequestUnits', 'Rank']
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
  
    # Copy IntervalEnd
    if 'IntervalEnd' in df.columns:
        result_df['IntervalEnd'] = df['IntervalEnd']
    else:
        # Use a default value if IntervalEnd is not available
        result_df['IntervalEnd'] = None
    
    # Transform CPUTime: Min = Max = Value, Sum = Value
    if 'CPUTime' in df.columns:
        result_df['MinCPUTime'] = df['CPUTime'].astype(float)
        result_df['MaxCPUTime'] = df['CPUTime'].astype(float)
        result_df['SumCPUTime'] = df['CPUTime'].astype(float)
    else:
        result_df['MinCPUTime'] = 0.0
        result_df['MaxCPUTime'] = 0.0
        result_df['SumCPUTime'] = 0.0
    
    # Transform Duration: Min = Max = Value, Sum = Value
    if 'Duration' in df.columns:
        result_df['MinDuration'] = df['Duration'].astype(float)
        result_df['MaxDuration'] = df['Duration'].astype(float)
        result_df['SumDuration'] = df['Duration'].astype(float)
    else:
        result_df['MinDuration'] = 0.0
        result_df['MaxDuration'] = 0.0
        result_df['SumDuration'] = 0.0
    
    # Transform ReadRows: Min = Max = Value, Sum = Value
    if 'ReadRows' in df.columns:
        result_df['MinReadRows'] = df['ReadRows'].astype(float)
        result_df['MaxReadRows'] = df['ReadRows'].astype(float)
        result_df['SumReadRows'] = df['ReadRows'].astype(float)
    else:
        result_df['MinReadRows'] = 0.0
        result_df['MaxReadRows'] = 0.0
        result_df['SumReadRows'] = 0.0
    
    # Transform ReadBytes: Min = Max = Value, Sum = Value
    if 'ReadBytes' in df.columns:
        result_df['MinReadBytes'] = df['ReadBytes'].astype(float)
        result_df['MaxReadBytes'] = df['ReadBytes'].astype(float)
        result_df['SumReadBytes'] = df['ReadBytes'].astype(float)
    else:
        result_df['MinReadBytes'] = 0.0
        result_df['MaxReadBytes'] = 0.0
        result_df['SumReadBytes'] = 0.0
    
    # Transform UpdateRows: Min = Max = Value, Sum = Value
    if 'UpdateRows' in df.columns:
        result_df['MinUpdateRows'] = df['UpdateRows'].astype(float)
        result_df['MaxUpdateRows'] = df['UpdateRows'].astype(float)
        result_df['SumUpdateRows'] = df['UpdateRows'].astype(float)
    else:
        result_df['MinUpdateRows'] = 0.0
        result_df['MaxUpdateRows'] = 0.0
        result_df['SumUpdateRows'] = 0.0
    
    # Transform UpdateBytes: Min = Max = Value, Sum = Value
    if 'UpdateBytes' in df.columns:
        result_df['MinUpdateBytes'] = df['UpdateBytes'].astype(float)
        result_df['MaxUpdateBytes'] = df['UpdateBytes'].astype(float)
        result_df['SumUpdateBytes'] = df['UpdateBytes'].astype(float)
    else:
        result_df['MinUpdateBytes'] = 0.0
        result_df['MaxUpdateBytes'] = 0.0
        result_df['SumUpdateBytes'] = 0.0
    
    # Copy QueryText and Rank
    if 'QueryText' in df.columns:
        result_df['QueryText'] = df['QueryText']
    else:
        result_df['QueryText'] = ''
    
    if 'Rank' in df.columns:
        result_df['Rank'] = df['Rank'].astype(int)
    else:
        result_df['Rank'] = 0

    # Set Count = 1 for each row (after handling header row)
    result_df['Count'] = 1.0  # Use float to ensure consistent type
  
    return result_df


def detect_encoding(file_path: str) -> str:
    """
    Detect the encoding of a file by checking for byte order marks (BOM).
    
    Args:
        file_path: Path to the file
        
    Returns:
        Detected encoding or 'utf-8' as default
    """
    encodings = {
        b'\xff\xfe': 'utf-16le',  # UTF-16 Little Endian
        b'\xfe\xff': 'utf-16be',  # UTF-16 Big Endian
        b'\xef\xbb\xbf': 'utf-8-sig',  # UTF-8 with BOM
    }
    
    with open(file_path, 'rb') as f:
        raw = f.read(4)  # Read first 4 bytes to check for BOM
        
    for bom, encoding in encodings.items():
        if raw.startswith(bom):
            return encoding
    
    return 'utf-8'  # Default to UTF-8 if no BOM is detected


def has_headers(sample_df: pd.DataFrame) -> bool:
    """
    Determine if a DataFrame sample has headers.
    
    Args:
        sample_df: DataFrame sample (usually first row)
        
    Returns:
        True if the DataFrame has headers, False otherwise
    """
    if len(sample_df) == 0 or len(sample_df.columns) == 0:
        return False
    
    # Check for common header names in the columns
    header_indicators = ['QueryText', 'MaxDuration', 'Duration', 'IntervalEnd']
    if any(header in sample_df.columns for header in header_indicators):
        return True
    
    # Check if any of the values in the first row match common header names
    first_row_values = [str(val).strip() for val in sample_df.iloc[0].values if not pd.isna(val)]
    if any(header in first_row_values for header in header_indicators):
        return True
    
    return False


def detect_and_load_file(file_path: str, encoding: str, file_format: str = None) -> pd.DataFrame:
    """
    Detect file format and load the file with appropriate column names.
    
    Args:
        file_path: Path to the TSV file
        encoding: File encoding
        format_hint: Optional hint for file format
        
    Returns:
        DataFrame with the TSV data in query_metrics format
    """
    sample_df = pd.read_csv(file_path, sep='\t', encoding=encoding, nrows=5, header=None)   

    if not file_format:
        file_format = detect_file_format(sample_df)

    # Check if the file has headers
    headers_present = has_headers(sample_df)
    
    # Determine file format
    if headers_present:
        # Read the file with headers
        df = pd.read_csv(file_path, sep='\t', encoding=encoding)
    else:
        column_names = TOP_QUERIES_COLUMNS if file_format == 'top_queries' else QUERY_METRICS_COLUMNS
        df = pd.read_csv(file_path, sep='\t', header=None, names=column_names, encoding=encoding)
    
    # Transform data if needed
    if file_format == 'top_queries':
        df = transform_top_queries_to_query_metrics(df)
    
    return df


def load_tsv_file(file_path: str, format_hint: str = None) -> pd.DataFrame:
    """
    Load a TSV file using pandas.
    
    Args:
        file_path: Path to the TSV file
        format_hint: Optional hint for file format ('query_metrics' or 'top_queries')
        
    Returns:
        DataFrame with the TSV data in query_metrics format
    """
    # Detect file encoding
    encoding = detect_encoding(file_path)
    
    try:
        # Try to load the file with the detected encoding
        return detect_and_load_file(file_path, encoding, format_hint)
    except Exception as e:
        # If the first attempt fails, try with different encodings
        for fallback_encoding in ['utf-8', 'utf-16le', 'utf-16be', 'latin1']:
            if fallback_encoding != encoding:
                try:
                    return detect_and_load_file(file_path, fallback_encoding, format_hint)
                except Exception:
                    continue
        
        # If all attempts fail, raise the original exception
        raise e