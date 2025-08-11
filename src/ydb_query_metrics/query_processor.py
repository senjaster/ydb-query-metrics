#!/usr/bin/env python3
"""
Query Processor Module for Query Metrics Processor

Contains the main processing logic for handling query metrics files.
"""

import click
import pandas as pd
from typing import List, Dict, Tuple

from ydb_query_metrics.file_format import load_tsv_file
from ydb_query_metrics.query_filter import filter_queries
from ydb_query_metrics.formatting import print_queries_to_console, write_sql_files, write_query_with_stats, get_sort_key
from ydb_query_metrics.query_statistics import calculate_statistics


def process_files(file_paths: List[str], like_filters: List[str], not_like_filters: List[str],
                 regex_filters: List[str] = None, output_dir: str = None, no_format: bool = False,
                 one_file: bool = False, to_stdout: bool = False, format_hint: str = None,
                 sort_by: str = 'MaxDuration', output_file: str = None) -> None:
    """
    Process multiple TSV files.
    
    Args:
        file_paths: List of file paths to process
        like_filters: List of patterns to include (substring match)
        not_like_filters: List of patterns to exclude (substring match)
        regex_filters: List of regular expressions to match
        output_dir: Directory to write SQL files to (when not using output_file)
        no_format: Whether to disable SQL formatting
        one_file: Whether to write all queries to a single file
        to_stdout: Whether to write output to stdout instead of files
        format_hint: Optional hint for file format ('query_metrics' or 'top_queries')
        sort_by: Metric to sort queries by ('MaxDuration', 'AvgDuration', 'MaxCPUTime', 'AvgCPUTime')
        output_file: Path to a single output file for all queries (overrides one_file)
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
    
    if to_stdout:
        # Print to console
        print_queries_to_console(query_stats, no_format, sort_by)
    elif output_file:
        # Write all queries to a single specified file
        os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)
        with open(output_file, 'w') as f:
            # Sort queries by the specified metric (descending)
            sorted_queries = sorted(
                query_stats.items(),
                key=lambda x: get_sort_key(x[1], sort_by),
                reverse=True
            )
            
            for i, (query, stats) in enumerate(sorted_queries, 1):
                # Add a separator between queries
                if i > 1:
                    f.write("\n\n-- " + "=" * 120 + "\n\n")
                
                # Write query with statistics
                write_query_with_stats(f, query, stats, i, no_format, sort_by)
        
        click.echo(f"SQL queries written to {output_file}")
    else:
        # Write to files in output directory
        actual_output_dir = write_sql_files(query_stats, output_dir, no_format, one_file, sort_by)
        click.echo(f"SQL files written to {actual_output_dir}/")