import os
import pytest
import tempfile
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from ydb_query_metrics.cli import main


class TestCli:
    """Tests for the CLI module."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()

    @patch('ydb_query_metrics.cli.process_files')
    def test_cli_basic(self, mock_process):
        """Test basic CLI functionality."""
        # Run the CLI command
        result = self.runner.invoke(main, ['tests/fixtures/query_metrics_sample.tsv'])
        
        # Check that the command executed successfully
        assert result.exit_code == 0
        
        # Check that process_files was called with the correct arguments
        mock_process.assert_called_once_with(
            ['tests/fixtures/query_metrics_sample.tsv'],  # file_paths
            [],  # like_filters
            [],  # not_like_filters
            [],  # regex_filters
            None,  # output_dir (default value is now None)
            False,  # no_format
            False,  # one_file
            True,  # to_stdout (was to_files, now inverted)
            None,  # format_hint
            'MaxDuration',  # sort_by
            None,  # output_file
            False  # overwrite
        )

    @patch('ydb_query_metrics.cli.process_files')
    def test_cli_with_filters(self, mock_process):
        """Test CLI with filter options."""
        # Run the CLI command with filters
        result = self.runner.invoke(main, [
            'tests/fixtures/query_metrics_sample.tsv',
            '--like', 'table_alpha',
            '--not-like', 'system',
            '--regex', 'SELECT.*FROM'
        ])
        
        # Check that the command executed successfully
        assert result.exit_code == 0
        
        # Check that process_files was called with the correct arguments
        mock_process.assert_called_once_with(
            ['tests/fixtures/query_metrics_sample.tsv'],  # file_paths
            ['table_alpha'],  # like_filters
            ['system'],  # not_like_filters
            ['SELECT.*FROM'],  # regex_filters
            None,  # output_dir
            False,  # no_format
            False,  # one_file
            True,  # to_stdout
            None,  # format_hint
            'MaxDuration',  # sort_by
            None,  # output_file
            False  # overwrite
        )

    @patch('ydb_query_metrics.cli.process_files')
    def test_cli_with_output_options(self, mock_process):
        """Test CLI with output options."""
        # Run the CLI command with output options
        result = self.runner.invoke(main, [
            'tests/fixtures/query_metrics_sample.tsv',
            '--output', 'custom_output/all_queries.sql',
            '--keep-query-format'
        ])
        
        # Check that the command executed successfully
        assert result.exit_code == 0
        
        # Check that process_files was called with the correct arguments
        mock_process.assert_called_once_with(
            ['tests/fixtures/query_metrics_sample.tsv'],  # file_paths
            [],  # like_filters
            [],  # not_like_filters
            [],  # regex_filters
            None,  # output_dir (default value is now None)
            True,  # no_format
            True,  # one_file (derived from output being specified)
            True,  # to_stdout (actually "not to_stdout" in the CLI implementation)
            None,  # format_hint
            'MaxDuration',  # sort_by
            'custom_output/all_queries.sql',  # output_file
            False  # overwrite
        )

    @patch('ydb_query_metrics.cli.process_files')
    def test_cli_with_format_hint(self, mock_process):
        """Test CLI with format hint."""
        # Run the CLI command with format hint
        result = self.runner.invoke(main, [
            'tests/fixtures/query_metrics_sample.tsv',
            '--format', 'query_metrics'
        ])
        
        # Check that the command executed successfully
        assert result.exit_code == 0
        
        # Check that process_files was called with the correct arguments
        mock_process.assert_called_once_with(
            ['tests/fixtures/query_metrics_sample.tsv'],  # file_paths
            [],  # like_filters
            [],  # not_like_filters
            [],  # regex_filters
            None,  # output_dir
            False,  # no_format
            False,  # one_file
            True,  # to_stdout
            'query_metrics',  # format_hint
            'MaxDuration',  # sort_by
            None,  # output_file
            False  # overwrite
        )

    @patch('ydb_query_metrics.cli.process_files')
    def test_cli_multiple_files(self, mock_process):
        """Test CLI with multiple input files."""
        # Run the CLI command with multiple files
        result = self.runner.invoke(main, [
            'tests/fixtures/query_metrics_sample.tsv',
            'tests/fixtures/top_queries_sample.tsv'
        ])
        
        # Check that the command executed successfully
        assert result.exit_code == 0
        
        # Check that process_files was called with the correct arguments
        mock_process.assert_called_once_with(
            ['tests/fixtures/query_metrics_sample.tsv', 'tests/fixtures/top_queries_sample.tsv'],  # file_paths
            [],  # like_filters
            [],  # not_like_filters
            [],  # regex_filters
            None,  # output_dir
            False,  # no_format
            False,  # one_file
            True,  # to_stdout
            None,  # format_hint
            'MaxDuration',  # sort_by
            None,  # output_file
            False  # overwrite
        )

    @patch('ydb_query_metrics.cli.process_files')
    def test_cli_glob_patterns(self, mock_process):
        """Test CLI with glob patterns."""
        # Create test files to match the glob pattern
        with tempfile.NamedTemporaryFile(suffix='.tsv', dir='tests/fixtures', delete=False) as f:
            try:
                # Run the CLI command with the actual file
                result = self.runner.invoke(main, [f.name])
                
                # Check that process_files was called
                mock_process.assert_called_once()
                
                # Check that the first argument (file_paths) contains our file
                args, _ = mock_process.call_args
                assert f.name in args[0]
            finally:
                # Clean up the temporary file
                os.unlink(f.name)

    def test_cli_no_matching_files(self):
        """Test CLI with no matching files."""
        # Run the CLI command with a non-matching pattern
        # Use a pattern that's unlikely to match any files
        result = self.runner.invoke(main, ['tests/fixtures/nonexistent_file_12345.tsv'])
        
        # Check that the command failed with an error code
        assert result.exit_code != 0
        
        # Check that the error message is in the output
        assert "does not exist" in result.output

    def test_cli_missing_required_argument(self):
        """Test CLI with missing required argument."""
        # Run the CLI command without the required FILES argument
        result = self.runner.invoke(main, [])
        
        # Check that the command failed with an error
        assert result.exit_code != 0
        assert "Missing argument 'FILES...'" in result.output

    @patch('ydb_query_metrics.cli.process_files')
    def test_cli_with_sort_by_option(self, mock_process):
        """Test CLI with sort-by option."""
        # Run the CLI command with sort-by option
        result = self.runner.invoke(main, [
            'tests/fixtures/query_metrics_sample.tsv',
            '--sort-by', 'AvgCPUTime'
        ])
        
        # Check that the command executed successfully
        assert result.exit_code == 0
        
        # Check that process_files was called with the correct arguments
        mock_process.assert_called_once_with(
            ['tests/fixtures/query_metrics_sample.tsv'],  # file_paths
            [],  # like_filters
            [],  # not_like_filters
            [],  # regex_filters
            None,  # output_dir
            False,  # no_format
            False,  # one_file
            True,  # to_stdout
            None,  # format_hint
            'AvgCPUTime',  # sort_by
            None,  # output_file
            False  # overwrite
        )

    @patch('ydb_query_metrics.cli.process_files')
    def test_cli_multiple_filter_options(self, mock_process):
        """Test CLI with multiple instances of the same filter option."""
        # Run the CLI command with multiple filter options
        result = self.runner.invoke(main, [
            'tests/fixtures/query_metrics_sample.tsv',
            '--like', 'table_alpha',
            '--like', 'SELECT',
            '--not-like', 'system',
            '--not-like', 'temp',
            '--regex', 'SELECT.*FROM',
            '--regex', 'WHERE.*='
        ])
        
        # Check that the command executed successfully
        assert result.exit_code == 0
        
        # Check that process_files was called with the correct arguments
        mock_process.assert_called_once_with(
            ['tests/fixtures/query_metrics_sample.tsv'],  # file_paths
            ['table_alpha', 'SELECT'],  # like_filters
            ['system', 'temp'],  # not_like_filters
            ['SELECT.*FROM', 'WHERE.*='],  # regex_filters
            None,  # output_dir
            False,  # no_format
            False,  # one_file
            True,  # to_stdout
            None,  # format_hint
            'MaxDuration',  # sort_by
            None,  # output_file
            False  # overwrite
        )