#!/usr/bin/env python3
"""
Formatting Module for Query Metrics Processor

Contains functions for formatting and outputting query data with statistics.
"""

import os
import click
import sqlparse
import datetime
from typing import Dict, TextIO
from ydb_query_metrics.query_statistics import QueryStatistics


def format_number_with_suffix(value: float) -> str:
    """
    Format a number with appropriate suffix (k, M, G, T, P).
    
    Args:
        value: The number to format
        
    Returns:
        Formatted string with appropriate suffix
    """
    suffixes = ['', 'k', 'M', 'G', 'T', 'P']
    suffix_index = 0
    
    # Handle special case for zero
    if value == 0:
        return "0"
        
    # Find appropriate suffix
    while value >= 1000 and suffix_index < len(suffixes) - 1:
        value /= 1000
        suffix_index += 1
    
    # Format with appropriate precision
    if value >= 100:
        return f"{value:.0f}{suffixes[suffix_index]}"
    elif value >= 10:
        return f"{value:.1f}{suffixes[suffix_index]}"
    else:
        return f"{value:.2f}{suffixes[suffix_index]}"


def format_query_with_stats(query: str, stats: QueryStatistics, query_number: int = None, no_format: bool = False, sort_by: str = 'MaxDuration') -> str:
    """
    Helper function to format a query with its statistics.
    
    Args:
        query: SQL query text
        stats: Statistics for the query
        query_number: Optional query number for headers
        no_format: Whether to disable SQL formatting
        sort_by: Metric to sort queries by
        
    Returns:
        Formatted string with query and statistics
    """
    result = []
    
    # Add query header with number if provided
    if query_number is not None:
        # Show the sort metric in the header
        if sort_by == 'MaxDuration':
            result.append(f"-- Query #{query_number} (MaxDuration: {stats.duration.max:.6f} seconds)\n")
        elif sort_by == 'AvgDuration':
            result.append(f"-- Query #{query_number} (AvgDuration: {stats.duration.avg:.6f} seconds)\n")
        elif sort_by == 'MaxCPUTime':
            result.append(f"-- Query #{query_number} (MaxCPUTime: {stats.cpu_time.max:.6f} seconds)\n")
        elif sort_by == 'AvgCPUTime':
            result.append(f"-- Query #{query_number} (AvgCPUTime: {stats.cpu_time.avg:.6f} seconds)\n")
    
    # Add statistics as a pivot table in a comment block
    result.append("/*")
    result.append(f"Row count: {stats.row_count}")
    result.append(f"Total count: {stats.total_count}\n")
    
    # Create a pivot table header
    result.append(f"{'Statistic':<15} {'Min':<15} {'Avg':<15} {'Max':<15}")
    result.append(f"{'-'*15} {'-'*15} {'-'*15} {'-'*15}")
    
    # Add rows for each statistic
    # Duration
    result.append(f"{stats.duration.metric_name:<15} {stats.duration.min:<15.6f} {stats.duration.avg:<15.6f} {stats.duration.max:<15.6f}")
    
    # CPUTime
    result.append(f"{stats.cpu_time.metric_name:<15} {stats.cpu_time.min:<15.6f} {stats.cpu_time.avg:<15.6f} {stats.cpu_time.max:<15.6f}")
    
    # ReadRows
    min_read_rows = format_number_with_suffix(stats.read_rows.min)
    avg_read_rows = format_number_with_suffix(stats.read_rows.avg)
    max_read_rows = format_number_with_suffix(stats.read_rows.max)
    result.append(f"{stats.read_rows.metric_name:<15} {min_read_rows:<15} {avg_read_rows:<15} {max_read_rows:<15}")
    
    # ReadBytes
    min_read_bytes = format_number_with_suffix(stats.read_bytes.min)
    avg_read_bytes = format_number_with_suffix(stats.read_bytes.avg)
    max_read_bytes = format_number_with_suffix(stats.read_bytes.max)
    result.append(f"{stats.read_bytes.metric_name:<15} {min_read_bytes:<15} {avg_read_bytes:<15} {max_read_bytes:<15}")
    
    # UpdateRows
    min_update_rows = format_number_with_suffix(stats.update_rows.min)
    avg_update_rows = format_number_with_suffix(stats.update_rows.avg)
    max_update_rows = format_number_with_suffix(stats.update_rows.max)
    result.append(f"{stats.update_rows.metric_name:<15} {min_update_rows:<15} {avg_update_rows:<15} {max_update_rows:<15}")
    
    # UpdateBytes
    min_update_bytes = format_number_with_suffix(stats.update_bytes.min)
    avg_update_bytes = format_number_with_suffix(stats.update_bytes.avg)
    max_update_bytes = format_number_with_suffix(stats.update_bytes.max)
    result.append(f"{stats.update_bytes.metric_name:<15} {min_update_bytes:<15} {avg_update_bytes:<15} {max_update_bytes:<15}")
    
    # Add derived statistics
    result.append(f"{'-'*15} {'-'*15} {'-'*15} {'-'*15}")
    
    # Use the properties from QueryStatistics
    rows_per_second_formatted = format_number_with_suffix(stats.rows_per_second)
    result.append(f"{'Rows/second':<15} {'':<15} {rows_per_second_formatted:<15} {'':<15}")
    
    bytes_per_row_formatted = format_number_with_suffix(stats.bytes_per_row)
    result.append(f"{'Bytes/row':<15} {'':<15} {bytes_per_row_formatted:<15} {'':<15}")
    
    result.append("*/\n")
    
    # Replace escaped newlines with actual newlines
    processed_query = query.replace('\\n', '\n')
    
    if no_format:
        # Add the query without formatting
        result.append(processed_query)
        
        # Add a newline at the end of the query if needed
        if not processed_query.endswith('\n'):
            result.append('')
    else:
        # Format the SQL query using sqlparse
        formatted_query = sqlparse.format(
            processed_query,
            reindent=True,
            keyword_case='upper',
            indent_width=4,
            compact=True,
            wrap_after=80
        )
        
        # Add the formatted query
        result.append(formatted_query)
        
        # Add a newline at the end of the query if needed
        if not formatted_query.endswith('\n'):
            result.append('')
    
    return '\n'.join(result)


def write_query_with_stats(f: TextIO, query: str, stats: QueryStatistics, query_number: int = None, no_format: bool = False, sort_by: str = 'MaxDuration') -> None:
    """
    Helper function to write a query with its statistics to a file.
    
    Args:
        f: File object to write to
        query: SQL query text
        stats: Statistics for the query
        query_number: Optional query number for headers
        no_format: Whether to disable SQL formatting
        sort_by: Metric to sort queries by
    """
    formatted_content = format_query_with_stats(query, stats, query_number, no_format, sort_by)
    f.write(formatted_content)


def get_sort_key(stats: QueryStatistics, sort_by: str):
    """
    Get the appropriate sort key based on the sort_by parameter.
    
    Args:
        stats: QueryStatistics object
        sort_by: Metric to sort by
        
    Returns:
        The value to sort by
    """
    if sort_by == 'MaxDuration':
        return stats.duration.max
    elif sort_by == 'AvgDuration':
        return stats.duration.avg
    elif sort_by == 'MaxCPUTime':
        return stats.cpu_time.max
    elif sort_by == 'AvgCPUTime':
        return stats.cpu_time.avg
    else:
        # Default to MaxDuration
        return stats.duration.max


def print_queries_to_console(query_stats: Dict[str, QueryStatistics], no_format: bool = False, sort_by: str = 'MaxDuration') -> None:
    """
    Print queries with statistics to the console.
    
    Args:
        query_stats: Dictionary mapping query text to statistics
        no_format: Whether to disable SQL formatting
        sort_by: Metric to sort queries by
    """
    # Sort queries by the specified metric (descending)
    sorted_queries = sorted(
        query_stats.items(),
        key=lambda x: get_sort_key(x[1], sort_by),
        reverse=True
    )
    
    for i, (query, stats) in enumerate(sorted_queries, 1):
        # Add a separator between queries
        if i > 1:
            click.echo("\n" + "=" * 120 + "\n")
        
        # Format and print query with statistics
        formatted_query = format_query_with_stats(query, stats, i, no_format, sort_by)
        click.echo(formatted_query)


def write_sql_files(query_stats: Dict[str, QueryStatistics], output_dir: str = '.', no_format: bool = False, one_file: bool = False, sort_by: str = 'MaxDuration') -> str:
    """
    Write each query to a separate SQL file with statistics.
    
    Args:
        query_stats: Dictionary mapping query text to statistics
        output_dir: Directory to write SQL files to
        no_format: Whether to disable SQL formatting
        one_file: Whether to write all queries to a single file
        sort_by: Metric to sort queries by
        
    Returns:
        The path to the timestamped output directory
    """
    # Ensure base output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a timestamped subfolder
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_dir = os.path.join(output_dir, timestamp)
    os.makedirs(timestamped_dir, exist_ok=True)
    
    # Sort queries by the specified metric (descending)
    sorted_queries = sorted(
        query_stats.items(),
        key=lambda x: get_sort_key(x[1], sort_by),
        reverse=True
    )
    
    if one_file:
        # Write all queries to a single file
        file_path = os.path.join(timestamped_dir, "AllQueries.sql")
        with open(file_path, 'w') as f:
            for i, (query, stats) in enumerate(sorted_queries, 1):
                # Add a separator between queries
                if i > 1:
                    f.write("\n\n-- " + "=" * 120 + "\n\n")
                
                # Write query with statistics
                write_query_with_stats(f, query, stats, i, no_format, sort_by)
    else:
        # Write each query to a separate file
        for i, (query, stats) in enumerate(sorted_queries, 1):
            file_path = os.path.join(timestamped_dir, f"Query{i:03d}.sql")
            
            with open(file_path, 'w') as f:
                # Write query with statistics
                write_query_with_stats(f, query, stats, None, no_format, sort_by)
    
    return timestamped_dir