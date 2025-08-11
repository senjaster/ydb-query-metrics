#!/usr/bin/env python3
"""
CLI Module for Query Metrics Processor

Contains the command-line interface for the Query Metrics Processor.
"""

import glob
import click
from typing import Tuple

from ydb_query_metrics.query_processor import process_files


@click.command()
@click.argument('files', nargs=-1, required=True, type=click.Path(exists=True))
@click.option('--like', multiple=True, help='Filter queries containing this pattern (substring match, can be used multiple times, AND logic)')
@click.option('--not-like', multiple=True, help='Filter queries NOT containing this pattern (substring match, can be used multiple times, AND logic)')
@click.option('--regex', multiple=True, help='Filter queries matching this regular expression (can be used multiple times, AND logic)')
@click.option('--to-files', is_flag=True, help='Write output to files instead of console')
@click.option('--output-dir', default='output', help='Directory to write SQL files to (when using --to-files)')
@click.option('--no-format', is_flag=True, help='Disable SQL query formatting')
@click.option('--one-file', is_flag=True, help='Output all queries to a single file (when using --to-files)')
@click.option('--format', 'format_hint', type=click.Choice(['query_metrics', 'top_queries']), help='Specify the input file format')
@click.option('--sort-by', type=click.Choice(['MaxDuration', 'AvgDuration', 'MaxCPUTime', 'AvgCPUTime']), default='MaxDuration', help='Sort queries by this metric (default: MaxDuration)')
def main(files: Tuple[str], like: Tuple[str], not_like: Tuple[str], regex: Tuple[str], to_files: bool, output_dir: str, no_format: bool, one_file: bool, format_hint: str, sort_by: str) -> None:
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
    
    process_files(expanded_files, list(like), list(not_like), list(regex), output_dir, no_format, one_file, to_files, format_hint, sort_by)


if __name__ == '__main__':
    main()