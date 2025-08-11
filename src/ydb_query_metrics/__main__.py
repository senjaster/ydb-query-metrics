#!/usr/bin/env python3
"""
Main entry point for running the Query Metrics Processor as a module.

Example usage:
    python -m query_metrics_processor <arguments>
"""

from ydb_query_metrics.cli import main

if __name__ == '__main__':
    main()