"""
Query Metrics Processor

A package for processing TSV files containing SQL query execution statistics.
Extracts queries and prints them to console or writes them to separate SQL files with statistics.
"""

__version__ = '1.0.0'

# Import main components for easier access
from ydb_query_metrics.file_format import load_tsv_file
from ydb_query_metrics.query_filter import filter_queries
from ydb_query_metrics.formatting import format_query_with_stats, print_queries_to_console, write_multiple_sql_files, write_single_sql_file
from ydb_query_metrics.query_processor import process_files, OutputMode
from ydb_query_metrics.query_statistics import calculate_statistics, QueryStatistics, MetricStats