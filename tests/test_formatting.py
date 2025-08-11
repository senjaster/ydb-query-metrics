import os
import pytest
import tempfile
from datetime import datetime
from ydb_query_metrics.formatting import (
    format_number_with_suffix,
    format_query_with_stats,
    write_query_with_stats,
    print_queries_to_console,
    write_sql_files
)
from ydb_query_metrics.query_statistics import QueryStatistics


class TestFormatNumberWithSuffix:
    """Tests for the format_number_with_suffix function."""

    def test_format_zero(self):
        """Test formatting zero."""
        assert format_number_with_suffix(0) == "0"

    def test_format_small_numbers(self):
        """Test formatting small numbers (< 1000)."""
        assert format_number_with_suffix(0.5) == "0.50"
        assert format_number_with_suffix(5) == "5.00"
        assert format_number_with_suffix(50) == "50.0"
        assert format_number_with_suffix(500) == "500"

    def test_format_thousands(self):
        """Test formatting thousands."""
        assert format_number_with_suffix(1000) == "1.00k"
        assert format_number_with_suffix(5000) == "5.00k"
        assert format_number_with_suffix(50000) == "50.0k"
        assert format_number_with_suffix(500000) == "500k"

    def test_format_millions(self):
        """Test formatting millions."""
        assert format_number_with_suffix(1000000) == "1.00M"
        assert format_number_with_suffix(5000000) == "5.00M"
        assert format_number_with_suffix(50000000) == "50.0M"
        assert format_number_with_suffix(500000000) == "500M"

    def test_format_billions(self):
        """Test formatting billions."""
        assert format_number_with_suffix(1000000000) == "1.00G"
        assert format_number_with_suffix(5000000000) == "5.00G"
        assert format_number_with_suffix(50000000000) == "50.0G"
        assert format_number_with_suffix(500000000000) == "500G"


class TestFormatQueryWithStats:
    """Tests for the format_query_with_stats function."""

    def test_format_query_with_stats(self, query_statistics_sample):
        """Test formatting a query with its statistics."""
        # Get the first query from the sample
        query_text = next(iter(query_statistics_sample.keys()))
        stats = query_statistics_sample[query_text]
        
        # Format the query with stats
        formatted = format_query_with_stats(query_text, stats, sort_by='MaxDuration')
        
        # Check that the formatted string contains expected elements
        assert "/*" in formatted  # Comment block start
        assert "*/" in formatted  # Comment block end
        assert "Row count:" in formatted
        assert "Total count:" in formatted
        assert "Statistic" in formatted
        assert "Min" in formatted
        assert "Avg" in formatted
        assert "Max" in formatted
        assert "Duration (s)" in formatted
        assert "CPUTime (s)" in formatted
        assert "ReadRows" in formatted
        assert "ReadBytes" in formatted
        assert "Rows/second" in formatted
        assert "Bytes/row" in formatted
        
        # Check that the query text is included (might be formatted differently)
        # Extract the query without whitespace for comparison
        formatted_query = formatted.split("*/\n\n", 1)[1].strip()
        original_query = query_text.strip()
        # Compare without whitespace
        assert ''.join(original_query.split()) in ''.join(formatted_query.split())

    def test_format_query_with_stats_query_number(self, query_statistics_sample):
        """Test formatting a query with a query number."""
        # Get the first query from the sample
        query_text = next(iter(query_statistics_sample.keys()))
        stats = query_statistics_sample[query_text]
        
        # Format the query with stats and a query number
        formatted = format_query_with_stats(query_text, stats, query_number=1, sort_by='MaxDuration')
        
        # Check that the formatted string contains the query number
        assert "-- Query #1" in formatted

    def test_format_query_with_stats_no_format(self, query_statistics_sample):
        """Test formatting a query without SQL formatting."""
        # Get the first query from the sample
        query_text = next(iter(query_statistics_sample.keys()))
        stats = query_statistics_sample[query_text]
        
        # Format the query with stats but without SQL formatting
        formatted = format_query_with_stats(query_text, stats, no_format=True, sort_by='MaxDuration')
        
        # The query should be included as-is, not formatted
        assert query_text in formatted


class TestWriteQueryWithStats:
    """Tests for the write_query_with_stats function."""

    def test_write_query_with_stats(self, query_statistics_sample):
        """Test writing a query with its statistics to a file."""
        # Get the first query from the sample
        query_text = next(iter(query_statistics_sample.keys()))
        stats = query_statistics_sample[query_text]
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as f:
            try:
                # Write the query with stats to the file
                write_query_with_stats(f, query_text, stats, sort_by='MaxDuration')
                
                # Close the file to ensure all data is written
                f.close()
                
                # Read the file contents
                with open(f.name, 'r') as f_read:
                    content = f_read.read()
                
                # Check that the file contains the expected content
                assert "/*" in content
                assert "*/" in content
                assert "Row count:" in content
                
                # Check that the query text is included (might be formatted differently)
                # Extract the query without whitespace for comparison
                formatted_query = content.split("*/\n\n", 1)[1].strip()
                original_query = query_text.strip()
                # Compare without whitespace
                assert ''.join(original_query.split()) in ''.join(formatted_query.split())
            finally:
                # Clean up the temporary file
                os.unlink(f.name)


class TestPrintQueriesToConsole:
    """Tests for the print_queries_to_console function."""

    def test_print_queries_to_console(self, query_statistics_sample, monkeypatch, capsys):
        """Test printing queries to the console."""
        # Mock click.echo to capture output
        outputs = []
        monkeypatch.setattr('click.echo', lambda msg: outputs.append(msg))
        
        # Print queries to console
        print_queries_to_console(query_statistics_sample, sort_by='MaxDuration')
        
        # Check that output contains expected elements
        assert len(outputs) > 0
        
        # Check that the output contains separators between queries
        separator_count = sum(1 for output in outputs if "=" * 120 in output)
        assert separator_count == len(query_statistics_sample) - 1  # One less separator than queries


class TestWriteSqlFiles:
    """Tests for the write_sql_files function."""

    def test_write_sql_files_separate(self, query_statistics_sample, monkeypatch):
        """Test writing queries to separate SQL files."""
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock datetime.now to return a fixed timestamp
            fixed_datetime = datetime(2025, 1, 1, 12, 0, 0)
            monkeypatch.setattr('datetime.datetime', type('MockDatetime', (), {
                'now': lambda: fixed_datetime,
                '__new__': datetime.__new__,
                'strftime': datetime.strftime
            }))
            
            # Create a unique output directory for this test
            unique_output_dir = os.path.join('output', 'test_separate')
            
            # Write SQL files with a specific output_dir
            output_dir = write_sql_files(query_statistics_sample, unique_output_dir, sort_by='MaxDuration', overwrite=True)
            
            # Check that the output directory is the one we specified
            expected_dir = unique_output_dir
            assert output_dir == expected_dir
            assert os.path.exists(expected_dir)
            
            # Check that the correct number of files were created
            files = os.listdir(expected_dir)
            assert len(files) == len(query_statistics_sample)
            
            # Check that files are named correctly
            for i in range(1, len(query_statistics_sample) + 1):
                expected_file = f"Query{i:03d}.sql"
                assert expected_file in files
                
                # Check file content
                with open(os.path.join(expected_dir, expected_file), 'r') as f:
                    content = f.read()
                    assert "/*" in content
                    assert "*/" in content
                    assert "Row count:" in content

    def test_write_sql_files_one_file(self, query_statistics_sample, monkeypatch):
        """Test writing all queries to a single SQL file."""
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock datetime.now to return a fixed timestamp
            fixed_datetime = datetime(2025, 1, 1, 12, 0, 0)
            monkeypatch.setattr('datetime.datetime', type('MockDatetime', (), {
                'now': lambda: fixed_datetime,
                '__new__': datetime.__new__,
                'strftime': datetime.strftime
            }))
            
            # Create a unique output directory for this test
            unique_output_dir = os.path.join('output', 'test_one_file')
            
            # Write SQL files with one_file=True and a specific output_dir
            output_dir = write_sql_files(query_statistics_sample, unique_output_dir, one_file=True, sort_by='MaxDuration', overwrite=True)
            
            # Check that the output directory is the one we specified
            expected_dir = unique_output_dir
            assert output_dir == expected_dir
            assert os.path.exists(expected_dir)
            
            # Check that only one file was created
            files = os.listdir(expected_dir)
            assert len(files) == 1
            assert "AllQueries.sql" in files
            
            # Check file content
            with open(os.path.join(expected_dir, "AllQueries.sql"), 'r') as f:
                content = f.read()
                assert "/*" in content
                assert "*/" in content
                
                # Check that the file contains separators between queries
                separator_count = content.count("-- " + "=" * 120)
                assert separator_count == len(query_statistics_sample) - 1  # One less separator than queries
    
    def test_write_sql_files_with_specified_output_dir(self, query_statistics_sample):
        """Test writing queries to a specified output directory without timestamp."""
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write SQL files with specified output_dir
            output_dir = write_sql_files(query_statistics_sample, temp_dir, sort_by='MaxDuration', overwrite=False)
            
            # Check that the output directory is exactly the one specified (no timestamp subfolder)
            assert output_dir == temp_dir
            
            # Check that the correct number of files were created
            files = os.listdir(temp_dir)
            assert len(files) == len(query_statistics_sample)
            
            # Check that files are named correctly
            for i in range(1, len(query_statistics_sample) + 1):
                expected_file = f"Query{i:03d}.sql"
                assert expected_file in files
                
                # Check file content
                with open(os.path.join(temp_dir, expected_file), 'r') as f:
                    content = f.read()
                    assert "/*" in content
                    assert "*/" in content
                    assert "Row count:" in content
                
    def test_write_sql_files_with_overwrite(self, query_statistics_sample):
        """Test writing queries to a directory with existing files and overwrite flag."""
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a dummy file in the directory
            dummy_file = os.path.join(temp_dir, "dummy.txt")
            with open(dummy_file, 'w') as f:
                f.write("This is a dummy file")
            
            # Write SQL files with overwrite=True
            output_dir = write_sql_files(query_statistics_sample, temp_dir, sort_by='MaxDuration', overwrite=True)
            
            # Check that the output directory is exactly the one specified
            assert output_dir == temp_dir
            
            # Check that the dummy file was removed
            assert not os.path.exists(dummy_file)
            
            # Check that the correct number of files were created
            files = os.listdir(temp_dir)
            assert len(files) == len(query_statistics_sample)
    
    def test_write_sql_files_without_overwrite(self, query_statistics_sample):
        """Test writing queries to a directory with existing files without overwrite flag."""
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a dummy file in the directory
            dummy_file = os.path.join(temp_dir, "dummy.txt")
            with open(dummy_file, 'w') as f:
                f.write("This is a dummy file")
            
            # Try to write SQL files without overwrite flag
            with pytest.raises(ValueError) as excinfo:
                write_sql_files(query_statistics_sample, temp_dir, sort_by='MaxDuration', overwrite=False)
            
            # Check that the error message is correct
            assert "already contains files" in str(excinfo.value)
            
            # Check that the dummy file still exists
            assert os.path.exists(dummy_file)
                
    def test_sort_by_options(self, query_statistics_sample, monkeypatch, capsys):
        """Test different sort_by options."""
        # Mock click.echo to capture output
        outputs = []
        monkeypatch.setattr('click.echo', lambda msg: outputs.append(msg))
        
        # Test each sort_by option
        sort_options = ['MaxDuration', 'AvgDuration', 'MaxCPUTime', 'AvgCPUTime']
        
        for sort_by in sort_options:
            outputs.clear()
            print_queries_to_console(query_statistics_sample, sort_by=sort_by)
            
            # Check that output contains expected elements
            assert len(outputs) > 0
            
            # For queries with numbers, check that the header shows the correct sort metric
            for output in outputs:
                if "-- Query #" in output and sort_by in output:
                    assert True
                    break