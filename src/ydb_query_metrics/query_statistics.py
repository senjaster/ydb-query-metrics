#!/usr/bin/env python3
"""
Query Statistics module for Query Metrics Processor

Contains classes and methods for processing and representing query statistics.
"""

from typing import Dict, List, Optional, Any
import pandas as pd


class MetricStats:
    """
    Class representing statistics for a single metric (Duration, CPUTime, etc.)
    
    Attributes:
        min: Minimum value
        max: Maximum value
        sum: Sum of all values
        count: Number of values (optional)
        scale: Scale factor for unit conversion (e.g., 1_000_000 for ns to s)
        min_column: Column name for minimum value in DataFrame
        max_column: Column name for maximum value in DataFrame
        sum_column: Column name for sum value in DataFrame
        metric_name: Display name for the metric (e.g., 'Duration (s)')
    """
    def __init__(
        self, 
        min_column: str = "", 
        max_column: str = "", 
        sum_column: str = "", 
        scale: float = 1.0,
        metric_name: str = ""
    ):
        self.min = float('inf')
        self.max = 0.0
        self.sum = 0.0
        self.count = 0.0
        self.scale = scale
        self.min_column = min_column
        self.max_column = max_column
        self.sum_column = sum_column
        self.metric_name = metric_name
        self._total_count = 0.0
    
    @staticmethod
    def create_for_metric(metric_name: str, scale: float = 1.0) -> 'MetricStats':
        """
        Create a MetricStats instance for a specific metric using naming conventions.
        
        Args:
            metric_name: Base name of the metric (e.g., 'Duration', 'CPUTime')
            scale: Scale factor for unit conversion
            
        Returns:
            Configured MetricStats instance
        """
        # Create display name based on the metric name
        display_name = metric_name
        # Add unit suffix for time-based metrics
        if metric_name in ('Duration', 'CPUTime'):
            display_name = f"{metric_name} (s)"
                
        return MetricStats(
            min_column=f"Min{metric_name}",
            max_column=f"Max{metric_name}",
            sum_column=f"Sum{metric_name}",
            scale=scale,
            metric_name=display_name
        )
    
    @property
    def avg(self) -> float:
        """
        Calculate and return the average value.
        
        Returns:
            Average value (scaled appropriately)
        """
        if self._total_count > 0:
            return self.sum / (self._total_count * self.scale)
        return 0.0
    
    def update(self, row: pd.Series) -> None:
        """
        Update statistics with values from a DataFrame row.
        
        Args:
            row: DataFrame row with metric values
        """
        # Calculate count value from the row
        try:
            count_value = float(row['Count']) if not pd.isna(row['Count']) else 1.0
        except (ValueError, TypeError):
            count_value = 1.0
            
        # Update total count for average calculation
        self._total_count += count_value
        
        # Update min value (apply scaling for display)
        if self.min_column in row and not pd.isna(row[self.min_column]):
            self.min = min(self.min, row[self.min_column] / self.scale)
        
        # Update max value (apply scaling for display)
        if self.max_column in row and not pd.isna(row[self.max_column]):
            self.max = max(self.max, row[self.max_column] / self.scale)
        
        # Update sum value (keep original scale for calculation)
        if self.sum_column in row and not pd.isna(row[self.sum_column]):
            self.sum += row[self.sum_column]


class QueryStatistics:
    """
    Class representing statistics for a query.
    
    Attributes:
        query_text: The SQL query text
        row_count: Number of rows in the dataset for this query
        total_count: Total count from the Count column
        duration: Statistics for Duration
        cpu_time: Statistics for CPUTime
        read_rows: Statistics for ReadRows
        read_bytes: Statistics for ReadBytes
        update_rows: Statistics for UpdateRows
        update_bytes: Statistics for UpdateBytes
    """
    def __init__(self, query_text: str):
        self.query_text = query_text
        self.row_count = 0
        self.total_count = 0.0
        
        # Initialize metric statistics
        self.duration = MetricStats.create_for_metric('Duration', 1_000_000)  # nanoseconds to seconds
        self.cpu_time = MetricStats.create_for_metric('CPUTime', 1_000_000)   # nanoseconds to seconds
        self.read_rows = MetricStats.create_for_metric('ReadRows')
        self.read_bytes = MetricStats.create_for_metric('ReadBytes')
        self.update_rows = MetricStats.create_for_metric('UpdateRows')
        self.update_bytes = MetricStats.create_for_metric('UpdateBytes')
    
    @property
    def rows_per_second(self) -> float:
        """
        Calculate rows per second (avg read rows / avg duration).
        
        Returns:
            Rows per second value
        """
        if self.duration.avg > 0:
            return self.read_rows.avg / self.duration.avg
        return 0.0
    
    @property
    def bytes_per_row(self) -> float:
        """
        Calculate bytes per row (avg read bytes / avg read rows).
        
        Returns:
            Bytes per row value
        """
        if self.read_rows.avg > 0:
            return self.read_bytes.avg / self.read_rows.avg
        return 0.0
    
    def update_from_row(self, row: pd.Series) -> None:
        """
        Update statistics with values from a DataFrame row.
        
        Args:
            row: DataFrame row with metric values
        """
        self.row_count += 1
        
        # Ensure Count is a number and add it to total_count
        try:
            count_value = float(row['Count']) if not pd.isna(row['Count']) else 1.0
        except (ValueError, TypeError):
            count_value = 1.0
            
        self.total_count += count_value
        
        # Update each metric
        self.duration.update(row)
        self.cpu_time.update(row)
        self.read_rows.update(row)
        self.read_bytes.update(row)
        self.update_rows.update(row)
        self.update_bytes.update(row)


def calculate_statistics(df: pd.DataFrame) -> Dict[str, QueryStatistics]:
    """
    Calculate statistics for each unique query.
    
    Args:
        df: DataFrame of query data
        
    Returns:
        Dictionary mapping query text to QueryStatistics objects
    """
    # Ensure all numeric columns are properly converted to numbers
    numeric_columns = [
        'MinDuration', 'MaxDuration', 'SumDuration',
        'MinCPUTime', 'MaxCPUTime', 'SumCPUTime',
        'MinReadRows', 'MaxReadRows', 'SumReadRows',
        'MinReadBytes', 'MaxReadBytes', 'SumReadBytes',
        'MinUpdateRows', 'MaxUpdateRows', 'SumUpdateRows',
        'MinUpdateBytes', 'MaxUpdateBytes', 'SumUpdateBytes',
        'Count'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Group by QueryText
    unique_queries: Dict[str, QueryStatistics] = {}
    
    for _, row in df.iterrows():
        # Skip rows with empty or NaN QueryText
        if pd.isna(row['QueryText']) or str(row['QueryText']).strip() == '':
            continue
            
        query_text = str(row['QueryText'])
        
        if query_text not in unique_queries:
            unique_queries[query_text] = QueryStatistics(query_text=query_text)
        
        # Update statistics with values from this row
        unique_queries[query_text].update_from_row(row)
    
    return unique_queries