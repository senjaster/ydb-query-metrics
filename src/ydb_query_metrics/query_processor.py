#!/usr/bin/env python3
"""
Query Processor Module for Query Metrics Processor

Contains the main processing logic for handling query metrics files.
"""

import os
import click
import pandas as pd
from enum import Enum
from typing import List, Dict, Tuple, Optional

from ydb_query_metrics.file_format import load_tsv_file
from ydb_query_metrics.query_filter import filter_queries
from ydb_query_metrics.formatting import print_queries_to_console, write_multiple_sql_files, write_single_sql_file, get_sort_key
from ydb_query_metrics.query_statistics import calculate_statistics


class OutputMode(Enum):
    """Output mode for query processing results."""
    MULTIPLE_FILES = "multiple"  # Write to multiple files in default or specified directory
    SINGLE_FILE = "single"       # Write to a single file
    STDOUT = "stdout"            # Write to stdout


def process_files(file_paths: List[str], like_filters: List[str], not_like_filters: List[str],
                 regex_filters: List[str] = None, output_mode: OutputMode = OutputMode.MULTIPLE_FILES,
                 output_path: Optional[str] = None, no_format: bool = False, format_hint: str = None,
                 sort_by: str = 'MaxDuration', overwrite: bool = False) -> None:
    """
    Process multiple TSV files.
    
    Args:
        file_paths: List of file paths to process
        like_filters: List of patterns to include (substring match)
        not_like_filters: List of patterns to exclude (substring match)
        regex_filters: List of regular expressions to match
        output_mode: Mode for output (MULTIPLE_FILES, SINGLE_FILE, or STDOUT)
        output_path: Path for output (directory for MULTIPLE_FILES, file for SINGLE_FILE, ignored for STDOUT)
        no_format: Whether to disable SQL formatting
        format_hint: Optional hint for file format ('query_metrics' or 'top_queries')
        sort_by: Metric to sort queries by ('MaxDuration', 'AvgDuration', 'MaxCPUTime', 'AvgCPUTime')
        overwrite: Whether to overwrite existing files
    """
    # Combine data from all files
    all_data = pd.DataFrame()
    
    for file_path in file_paths:
        try:
            df = load_tsv_file(file_path, format_hint)
            all_data = pd.concat([all_data, df], ignore_index=True)
        except Exception as e:
            click.echo(f"Error processing file {file_path}: {e}", err=True)
    
    if all_data.empty:
        click.echo("No data found in the provided files.", err=True)
        return
    
    # Filter queries
    filtered_data = filter_queries(all_data, like_filters, not_like_filters, regex_filters)
    
    if filtered_data.empty:
        click.echo("No queries matched the filter criteria.", err=True)
        return
    
    # Calculate statistics
    query_stats = calculate_statistics(filtered_data)
    
    # Output results
    click.echo(f"Processed {len(all_data)} rows from {len(file_paths)} files.")
    click.echo(f"Found {len(query_stats)} unique queries after filtering.")
    
    if output_mode == OutputMode.STDOUT:
        # Print to console
        print_queries_to_console(query_stats, no_format, sort_by)
    elif output_mode == OutputMode.SINGLE_FILE:
        # Write to a single file
        if output_path:
            output_dir = write_single_sql_file(query_stats, output_path, no_format, sort_by)
            click.echo(f"SQL file written to {output_path}")
        else:
            # This should not happen due to CLI validation
            click.echo("Error: No output file specified for SINGLE_FILE mode", err=True)
    else:  # OutputMode.MULTIPLE_FILES
        # Write to multiple files in output directory
        actual_output_dir = write_multiple_sql_files(query_stats, output_path, no_format, sort_by, overwrite)
        click.echo(f"SQL files written to {actual_output_dir}/")