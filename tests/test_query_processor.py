import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from ydb_query_metrics.query_processor import process_files


class TestQueryProcessor:
    """Tests for the query_processor module."""

    @patch('ydb_query_metrics.query_processor.load_tsv_file')
    @patch('ydb_query_metrics.query_processor.filter_queries')
    @patch('ydb_query_metrics.query_processor.calculate_statistics')
    @patch('ydb_query_metrics.query_processor.print_queries_to_console')
    def test_process_files_console_output(
        self, mock_print, mock_calculate, mock_filter, mock_load, 
        query_metrics_df, query_statistics_sample
    ):
        """Test processing files with console output."""
        # Setup mocks
        mock_load.return_value = query_metrics_df
        mock_filter.return_value = query_metrics_df
        mock_calculate.return_value = query_statistics_sample
        
        # Call the function
        process_files(
            file_paths=['test_file.tsv'],
            like_filters=['table'],
            not_like_filters=['system'],
            regex_filters=None,
            output_dir=None,
            no_format=False,
            one_file=False,
            to_stdout=True,
            format_hint=None,
            sort_by='MaxDuration',
            output_file=None,
            overwrite=False
        )
        
        # Check that the mocks were called with expected arguments
        mock_load.assert_called_once_with('test_file.tsv', None)
        
        # Check filter arguments without direct DataFrame comparison
        args, kwargs = mock_filter.call_args
        assert len(args) == 4
        assert args[1] == ['table']
        assert args[2] == ['system']
        assert args[3] is None
        
        # Check calculate_statistics was called
        mock_calculate.assert_called_once()
        
        # Check print_queries_to_console was called with the right arguments
        mock_print.assert_called_once_with(query_statistics_sample, False, 'MaxDuration')

    @patch('ydb_query_metrics.query_processor.load_tsv_file')
    @patch('ydb_query_metrics.query_processor.filter_queries')
    @patch('ydb_query_metrics.query_processor.calculate_statistics')
    @patch('ydb_query_metrics.query_processor.write_sql_files')
    def test_process_files_file_output(
        self, mock_write, mock_calculate, mock_filter, mock_load, 
        query_metrics_df, query_statistics_sample
    ):
        """Test processing files with file output."""
        # Setup mocks
        mock_load.return_value = query_metrics_df
        mock_filter.return_value = query_metrics_df
        mock_calculate.return_value = query_statistics_sample
        mock_write.return_value = '/tmp/output'
        
        # Call the function
        process_files(
            file_paths=['test_file.tsv'],
            like_filters=[],
            not_like_filters=[],
            regex_filters=None,
            output_dir='output_dir',
            no_format=True,
            one_file=True,
            to_stdout=False,
            format_hint=None,
            sort_by='MaxDuration',
            output_file=None,
            overwrite=False
        )
        
        # Check that the mocks were called with expected arguments
        mock_load.assert_called_once_with('test_file.tsv', None)
        
        # Check filter arguments without direct DataFrame comparison
        args, kwargs = mock_filter.call_args
        assert len(args) == 4
        assert args[1] == []
        assert args[2] == []
        assert args[3] is None
        
        # Check calculate_statistics was called
        mock_calculate.assert_called_once()
        
        # Check write_sql_files was called with the right arguments
        mock_write.assert_called_once_with(query_statistics_sample, 'output_dir', True, True, 'MaxDuration', False)

    @patch('ydb_query_metrics.query_processor.load_tsv_file')
    @patch('ydb_query_metrics.query_processor.filter_queries')
    @patch('ydb_query_metrics.query_processor.calculate_statistics')
    @patch('ydb_query_metrics.query_processor.click.echo')
    def test_process_files_multiple_files(
        self, mock_echo, mock_calculate, mock_filter, mock_load, 
        query_metrics_df, query_statistics_sample
    ):
        """Test processing multiple files."""
        # Setup mocks
        mock_load.return_value = query_metrics_df
        mock_filter.return_value = query_metrics_df
        mock_calculate.return_value = query_statistics_sample
        
        # Call the function
        process_files(
            file_paths=['file1.tsv', 'file2.tsv'],
            like_filters=[],
            not_like_filters=[],
            regex_filters=None,
            output_dir=None,
            no_format=False,
            one_file=False,
            to_stdout=True,
            format_hint=None,
            sort_by='MaxDuration',
            output_file=None,
            overwrite=False
        )
        
        # Check that load_tsv_file was called for each file
        assert mock_load.call_count == 2
        mock_load.assert_any_call('file1.tsv', None)
        mock_load.assert_any_call('file2.tsv', None)
        
        # Check that the other functions were called once
        mock_filter.assert_called_once()
        mock_calculate.assert_called_once()
        
        # Check that the summary message was printed
        mock_echo.assert_any_call(f"Processed {len(query_metrics_df) * 2} rows from 2 files.")
        mock_echo.assert_any_call(f"Found {len(query_statistics_sample)} unique queries after filtering.")

    @patch('ydb_query_metrics.query_processor.load_tsv_file')
    @patch('ydb_query_metrics.query_processor.click.echo')
    def test_process_files_empty_data(self, mock_echo, mock_load):
        """Test processing files with empty data."""
        # Setup mocks
        mock_load.return_value = pd.DataFrame()
        
        # Call the function
        process_files(
            file_paths=['empty_file.tsv'],
            like_filters=[],
            not_like_filters=[],
            regex_filters=None,
            output_dir=None,
            no_format=False,
            one_file=False,
            to_stdout=True,
            format_hint=None,
            sort_by='MaxDuration',
            output_file=None,
            overwrite=False
        )
        
        # Check that the error message was printed
        mock_echo.assert_any_call("No data found in the provided files.", err=True)

    @patch('ydb_query_metrics.query_processor.load_tsv_file')
    @patch('ydb_query_metrics.query_processor.filter_queries')
    @patch('ydb_query_metrics.query_processor.click.echo')
    def test_process_files_no_matches(
        self, mock_echo, mock_filter, mock_load, query_metrics_df
    ):
        """Test processing files with no matches after filtering."""
        # Setup mocks
        mock_load.return_value = query_metrics_df
        mock_filter.return_value = pd.DataFrame()  # Empty DataFrame after filtering
        
        # Call the function
        process_files(
            file_paths=['test_file.tsv'],
            like_filters=['nonexistent'],
            not_like_filters=[],
            regex_filters=None,
            output_dir=None,
            no_format=False,
            one_file=False,
            to_stdout=True,
            format_hint=None,
            sort_by='MaxDuration',
            output_file=None,
            overwrite=False
        )
        
        # Check that the error message was printed
        mock_echo.assert_any_call("No queries matched the filter criteria.", err=True)

    @patch('ydb_query_metrics.query_processor.load_tsv_file')
    def test_process_files_with_format_hint(self, mock_load, query_metrics_df):
        """Test processing files with a format hint."""
        # Setup mocks
        mock_load.return_value = query_metrics_df
        
        # Call the function
        process_files(
            file_paths=['test_file.tsv'],
            like_filters=[],
            not_like_filters=[],
            regex_filters=None,
            output_dir=None,
            no_format=False,
            one_file=False,
            to_stdout=True,
            format_hint='query_metrics',
            sort_by='MaxDuration',
            output_file=None,
            overwrite=False
        )
        
        # Check that load_tsv_file was called with the format hint
        mock_load.assert_called_once_with('test_file.tsv', 'query_metrics')

    @patch('ydb_query_metrics.query_processor.load_tsv_file')
    @patch('ydb_query_metrics.query_processor.click.echo')
    def test_process_files_load_error(self, mock_echo, mock_load):
        """Test handling of load errors."""
        # Setup mock to raise an exception
        mock_load.side_effect = Exception("Test error")
        
        # Call the function
        process_files(
            file_paths=['error_file.tsv'],
            like_filters=[],
            not_like_filters=[],
            regex_filters=None,
            output_dir=None,
            no_format=False,
            one_file=False,
            to_stdout=True,
            format_hint=None,
            sort_by='MaxDuration',
            output_file=None,
            overwrite=False
        )
        
        # Check that the error message was printed
        mock_echo.assert_any_call("Error processing file error_file.tsv: Test error", err=True)
        mock_echo.assert_any_call("No data found in the provided files.", err=True)