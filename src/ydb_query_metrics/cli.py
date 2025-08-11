#!/usr/bin/env python3
"""
CLI Module for Query Metrics Processor

Contains the command-line interface for the Query Metrics Processor.
"""

import glob
import click
from typing import Tuple, Optional

from ydb_query_metrics.query_processor import process_files, OutputMode


@click.command()
@click.argument('files', nargs=-1, required=True, type=click.Path(exists=True))
@click.option('-l', '--like', multiple=True, help='Filter queries containing this pattern (substring match, can be used multiple times, AND logic)')
@click.option('-n', '--not-like', multiple=True, help='Filter queries NOT containing this pattern (substring match, can be used multiple times, AND logic)')
@click.option('-r', '--regex', multiple=True, help='Filter queries matching this regular expression (can be used multiple times, AND logic)')
@click.option('-o', '--output', default=None, help='Output file for all queries (use "-" for stdout)')
@click.option('-d', '--output-dir', default=None, help='Directory to write SQL files to (when not using --output)')
@click.option('-w', '--overwrite', is_flag=True, help='Overwrite existing files in output directory')
@click.option('-k', '--keep-query-format', 'no_format', is_flag=True, help='Disable SQL query formatting')
@click.option('-f', '--format', 'format_hint', type=click.Choice(['query_metrics', 'top_queries']), help='Specify the input file format')
@click.option('-s', '--sort-by', type=click.Choice(['MaxDuration', 'AvgDuration', 'MaxCPUTime', 'AvgCPUTime']), default='MaxDuration', help='Sort queries by this metric (default: MaxDuration)')
def main(files: Tuple[str], like: Tuple[str], not_like: Tuple[str], regex: Tuple[str], output: str, output_dir: str, overwrite: bool, no_format: bool, format_hint: str, sort_by: str) -> None:
    """
    Process TSV files containing SQL query execution statistics.
    
    FILES: One or more TSV files to process. Glob patterns are supported.
    """
    # Expand glob patterns
    expanded_files = []
    for file_pattern in files:
        matched_files = glob.glob(file_pattern)
        if not matched_files:
            click.echo(f"Warning: No files matched pattern '{file_pattern}'", err=True)
        expanded_files.extend(matched_files)
    
    if not expanded_files:
        click.echo("Error: No files to process.", err=True)
        return
    
    # Determine output mode and path based on parameters
    output_mode = None
    output_path = None
    
    # Check for invalid combinations
    if output is not None and output_dir is not None:
        click.echo("Error: Cannot specify both --output and --output-dir", err=True)
        return
    
    # Set output mode and path
    if output == "-":
        output_mode = OutputMode.STDOUT
        output_path = None
    elif output is not None:
        output_mode = OutputMode.SINGLE_FILE
        output_path = output
    else:
        output_mode = OutputMode.MULTIPLE_FILES
        output_path = output_dir
    
    process_files(
        expanded_files,
        list(like),
        list(not_like),
        list(regex),
        output_mode,
        output_path,
        no_format,
        format_hint,
        sort_by,
        overwrite
    )


if __name__ == '__main__':
    main()