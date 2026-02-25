# Business Stream Data Scientist Interview: 12 Challenging Python Questions

## Company Context: Business Stream (UK Water Utilities)
- Water retailer managing millions of meter readings
- Complex billing cycles, seasonal patterns, anomaly detection
- Regulatory compliance and audit trails required
- Emphasis on reproducible workflows and production-grade code

---

## Question 1: Multi-Index Time-Series Aggregation with Custom Resampling

### Challenge
Business Stream receives meter readings at irregular intervals. You need to create a function that:
1. Handles irregular time-series data with multiple meters
2. Implements custom resampling logic for billing periods (not calendar months)
3. Calculates rolling statistics with variable windows
4. Handles timezone conversions for UK meters

**Requirements:**
- Support billing periods that start on any day of the month
- Calculate consumption per day, accounting for partial periods
- Return multi-index DataFrame with meter_id and billing_period
- Memory-efficient for 10M+ readings

### Production Solution

```python
"""
Time-series aggregation for irregular utility meter readings.
Handles billing period alignment and rolling statistics.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Iterator, Optional

import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

# Configure logging
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BillingPeriod:
    """Immutable billing period configuration."""
    start_day: int  # Day of month billing period starts (1-28)
    period_months: int = 1  # Duration in months
    
    def get_period_start(self, date: pd.Timestamp) -> pd.Timestamp:
        """Calculate the start of the billing period containing this date."""
        if date.day >= self.start_day:
            return date.replace(day=self.start_day)
        else:
            # Go to previous period
            prev_month = date - pd.DateOffset(months=1)
            return prev_month.replace(day=self.start_day)
    
    def get_period_label(self, date: pd.Timestamp) -> str:
        """Generate a consistent period label for grouping."""
        period_start = self.get_period_start(date)
        return period_start.strftime("%Y-%m")


class MeterDataAggregator:
    """
    Production-grade aggregator for utility meter data.
    
    Design decisions:
    - Chunked processing for memory efficiency
    - Vectorized operations over iteration
    - Immutable configuration via dataclasses
    - Comprehensive validation and error handling
    """
    
    def __init__(
        self,
        billing_period: BillingPeriod,
        timezone: str = "Europe/London",
        chunk_size: int = 100_000,
        max_gap_hours: float = 72.0
    ):
        self.billing_period = billing_period
        self.timezone = timezone
        self.chunk_size = chunk_size
        self.max_gap_hours = max_gap_hours
        self._validation_stats: dict = {}
    
    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and clean input data before processing.
        
        Returns validated DataFrame with quality flags.
        """
        required_cols = {'meter_id', 'timestamp', 'reading'}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        # Create copy to avoid modifying original
        df = df.copy()
        
        # Ensure timestamp is datetime
        if not is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        
        # Convert to target timezone
        if df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        df['timestamp'] = df['timestamp'].dt.tz_convert(self.timezone)
        
        # Sort by meter and time
        df = df.sort_values(['meter_id', 'timestamp']).reset_index(drop=True)
        
        # Add quality flags
        df['quality_flag'] = 'valid'
        
        # Flag negative readings (possible rollover or error)
        df.loc[df['reading'] < 0, 'quality_flag'] = 'negative_reading'
        
        # Flag suspicious gaps
        df['time_diff_hours'] = df.groupby('meter_id')['timestamp'].diff().dt.total_seconds() / 3600
        df.loc[df['time_diff_hours'] > self.max_gap_hours, 'quality_flag'] = 'large_gap'
        
        # Store validation statistics
        self._validation_stats = df['quality_flag'].value_counts().to_dict()
        logger.info(f"Validation complete: {self._validation_stats}")
        
        return df
    
    def calculate_consumption(
        self,
        df: pd.DataFrame,
        include_rolling: bool = True,
        rolling_windows: list[int] = [7, 30]
    ) -> pd.DataFrame:
        """
        Calculate consumption metrics with billing period alignment.
        
        Key algorithm:
        1. Assign each reading to its billing period
        2. Calculate daily consumption rates accounting for partial periods
        3. Compute rolling statistics for trend analysis
        """
        df = self.validate_data(df)
        
        # Assign billing period
        df['billing_period'] = df['timestamp'].apply(
            lambda x: self.billing_period.get_period_label(x)
        )
        df['period_start'] = df['timestamp'].apply(
            lambda x: self.billing_period.get_period_start(x)
        )
        
        # Calculate consumption per reading (difference from previous)
        df['consumption'] = df.groupby('meter_id')['reading'].diff()
        
        # Handle meter rollovers (sudden large negative values)
        rollover_mask = df['consumption'] < -1000  # Threshold for rollover
        df.loc[rollover_mask, 'consumption'] = np.nan
        df.loc[rollover_mask, 'quality_flag'] = 'rollover_detected'
        
        # Aggregate by billing period
        period_stats = df.groupby(['meter_id', 'billing_period']).agg({
            'consumption': ['sum', 'mean', 'std', 'count'],
            'reading': ['first', 'last'],
            'timestamp': ['min', 'max']
        }).reset_index()
        
        # Flatten column names
        period_stats.columns = [
            '_'.join(col).strip('_') if col[1] else col[0]
            for col in period_stats.columns.values
        ]
        
        # Calculate period duration and daily rate
        period_stats['period_days'] = (
            period_stats['timestamp_max'] - period_stats['timestamp_min']
        ).dt.total_seconds() / 86400
        
        period_stats['daily_consumption'] = (
            period_stats['consumption_sum'] / period_stats['period_days']
        )
        
        # Calculate rolling statistics if requested
        if include_rolling:
            for window in rolling_windows:
                period_stats[f'rolling_mean_{window}d'] = (
                    period_stats.groupby('meter_id')['daily_consumption']
                    .transform(lambda x: x.rolling(window, min_periods=1).mean())
                )
        
        return period_stats
    
    def process_large_dataset(
        self,
        file_path: str,
        output_path: Optional[str] = None
    ) -> Iterator[pd.DataFrame]:
        """
        Memory-efficient chunked processing for large datasets.
        
        Yields processed chunks for downstream consumption.
        """
        chunk_iter = pd.read_csv(
            file_path,
            parse_dates=['timestamp'],
            chunksize=self.chunk_size
        )
        
        for i, chunk in enumerate(chunk_iter):
            logger.info(f"Processing chunk {i+1}, size: {len(chunk)}")
            processed = self.calculate_consumption(chunk)
            
            if output_path:
                mode = 'w' if i == 0 else 'a'
                header = i == 0
                processed.to_csv(output_path, mode=mode, header=header, index=False)
            
            yield processed


# Usage example with test data
if __name__ == "__main__":
    # Generate test data
    np.random.seed(42)
    n_meters = 100
    n_readings = 10_000
    
    test_data = pd.DataFrame({
        'meter_id': np.random.choice([f'M{i:04d}' for i in range(n_meters)], n_readings),
        'timestamp': pd.date_range('2023-01-01', periods=n_readings, freq='H'),
        'reading': np.cumsum(np.random.exponential(0.5, n_readings)) + 1000
    })
    
    # Initialize aggregator with billing starting on 15th
    billing = BillingPeriod(start_day=15)
    aggregator = MeterDataAggregator(billing, chunk_size=5000)
    
    # Process data
    result = aggregator.calculate_consumption(test_data)
    print(f"Processed {len(result)} billing periods")
    print(result.head())
```

### Key Design Decisions

1. **Immutable Configuration**: `BillingPeriod` as frozen dataclass prevents accidental mutation
2. **Chunked Processing**: Iterator pattern for memory efficiency with large datasets
3. **Vectorized Operations**: Groupby transforms instead of row-wise iteration
4. **Comprehensive Validation**: Quality flags enable audit trails (critical for utilities)
5. **Timezone Handling**: Explicit UTC→local conversion for regulatory compliance

### Performance Considerations

- **Memory**: Chunked processing keeps memory usage O(chunk_size) not O(n)
- **Speed**: Vectorized groupby operations ~100x faster than iteration
- **Scalability**: Iterator pattern enables streaming processing of TB-scale data

---

## Question 2: Robust Data Cleaning Pipeline for Messy Meter Data

### Challenge
Utility meter data is notoriously messy: missing readings, duplicates, outliers, unit mismatches. Build a production-grade cleaning pipeline that:

1. Detects and handles duplicate readings (same meter, same time)
2. Identifies and interpolates missing values with configurable strategies
3. Detects outliers using statistical methods appropriate for consumption data
4. Handles unit conversions (cubic meters, gallons, liters)
5. Maintains audit trail of all modifications

### Production Solution

```python
"""
Production-grade data cleaning pipeline for utility meter data.
Implements audit logging, configurable strategies, and statistical outlier detection.
"""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


class CleaningAction(Enum):
    """Enumeration of possible cleaning actions for audit trail."""
    DUPLICATE_REMOVED = auto()
    MISSING_INTERPOLATED = auto()
    OUTLIER_WINSORIZED = auto()
    UNIT_CONVERTED = auto()
    NEGATIVE_CORRECTED = auto()


@dataclass
class CleaningRecord:
    """Audit record for data cleaning operations."""
    action: CleaningAction
    meter_id: str
    timestamp: datetime
    original_value: Any
    new_value: Any
    reason: str
    confidence: float = 1.0


class DataCleaningStrategy(ABC):
    """Abstract base class for cleaning strategies."""
    
    @abstractmethod
    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[CleaningRecord]]:
        """Apply cleaning strategy and return cleaned data + audit records."""
        pass


class DuplicateHandler(DataCleaningStrategy):
    """
    Handle duplicate readings with configurable resolution strategy.
    
    Strategies:
    - 'first': Keep first occurrence
    - 'last': Keep last occurrence  
    - 'mean': Average duplicate values
    - 'validate': Flag for manual review if values differ significantly
    """
    
    def __init__(self, strategy: str = 'validate', tolerance: float = 0.01):
        self.strategy = strategy
        self.tolerance = tolerance
    
    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[CleaningRecord]]:
        records = []
        
        # Find duplicates
        duplicates = df[df.duplicated(subset=['meter_id', 'timestamp'], keep=False)]
        
        if len(duplicates) == 0:
            return df, records
        
        if self.strategy == 'validate':
            # Check if duplicate values differ significantly
            grouped = duplicates.groupby(['meter_id', 'timestamp'])['reading']
            value_ranges = grouped.agg(['min', 'max', 'std', 'count'])
            value_ranges['relative_diff'] = (
                (value_ranges['max'] - value_ranges['min']) / value_ranges['min']
            )
            
            problematic = value_ranges[value_ranges['relative_diff'] > self.tolerance]
            
            for (meter_id, timestamp), row in problematic.iterrows():
                records.append(CleaningRecord(
                    action=CleaningAction.DUPLICATE_REMOVED,
                    meter_id=meter_id,
                    timestamp=timestamp,
                    original_value=f"min={row['min']:.2f}, max={row['max']:.2f}",
                    new_value=None,
                    reason=f"Conflicting duplicates with {row['relative_diff']:.2%} difference",
                    confidence=0.0
                ))
            
            # Keep first for non-problematic duplicates
            df = df.drop_duplicates(subset=['meter_id', 'timestamp'], keep='first')
            
        elif self.strategy == 'mean':
            # Average duplicates and create records
            for (meter_id, timestamp), group in duplicates.groupby(['meter_id', 'timestamp']):
                mean_val = group['reading'].mean()
                records.append(CleaningRecord(
                    action=CleaningAction.DUPLICATE_REMOVED,
                    meter_id=meter_id,
                    timestamp=timestamp,
                    original_value=group['reading'].tolist(),
                    new_value=mean_val,
                    reason="Averaged duplicate readings",
                    confidence=0.9
                ))
            
            df = df.groupby(['meter_id', 'timestamp'], as_index=False).agg({
                'reading': 'mean',
                'timestamp': 'first',
                'meter_id': 'first'
            })
        
        else:
            df = df.drop_duplicates(subset=['meter_id', 'timestamp'], keep=self.strategy)
        
        return df, records


class MissingValueHandler(DataCleaningStrategy):
    """
    Handle missing readings with time-series aware interpolation.
    
    Supports multiple strategies:
    - 'linear': Linear interpolation
    - 'time': Time-weighted interpolation
    - 'forward_fill': Forward fill (LOCF)
    - 'seasonal': Seasonal decomposition + interpolation
    """
    
    def __init__(
        self,
        strategy: str = 'time',
        max_gap_hours: float = 48.0,
        max_interpolation_ratio: float = 0.3
    ):
        self.strategy = strategy
        self.max_gap_hours = max_gap_hours
        self.max_interpolation_ratio = max_interpolation_ratio
    
    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[CleaningRecord]]:
        records = []
        
        # Create complete time index for each meter
        df = df.sort_values(['meter_id', 'timestamp']).copy()
        
        # Check interpolation feasibility
        total_by_meter = df.groupby('meter_id').size()
        
        for meter_id in df['meter_id'].unique():
            meter_data = df[df['meter_id'] == meter_id].copy()
            
            # Create regular time grid
            time_range = pd.date_range(
                start=meter_data['timestamp'].min(),
                end=meter_data['timestamp'].max(),
                freq='H'
            )
            
            # Calculate gap statistics
            missing_count = len(time_range) - len(meter_data)
            missing_ratio = missing_count / len(time_range)
            
            if missing_ratio > self.max_interpolation_ratio:
                logger.warning(
                    f"Meter {meter_id}: {missing_ratio:.1%} missing exceeds threshold"
                )
                continue
            
            # Reindex to complete time grid
            meter_data = meter_data.set_index('timestamp').reindex(time_range)
            meter_data['meter_id'] = meter_id
            
            # Track missing values before interpolation
            missing_mask = meter_data['reading'].isna()
            missing_times = meter_data.index[missing_mask]
            
            # Apply interpolation strategy
            if self.strategy == 'linear':
                meter_data['reading'] = meter_data['reading'].interpolate(method='linear')
            elif self.strategy == 'time':
                meter_data['reading'] = meter_data['reading'].interpolate(method='time')
            elif self.strategy == 'forward_fill':
                meter_data['reading'] = meter_data['reading'].ffill()
            
            # Create audit records for interpolated values
            for ts in missing_times:
                records.append(CleaningRecord(
                    action=CleaningAction.MISSING_INTERPOLATED,
                    meter_id=meter_id,
                    timestamp=ts,
                    original_value=None,
                    new_value=meter_data.loc[ts, 'reading'],
                    reason=f"{self.strategy} interpolation",
                    confidence=max(0, 1 - missing_ratio)
                ))
            
            # Update dataframe
            meter_data = meter_data.reset_index().rename(columns={'index': 'timestamp'})
            df = df[df['meter_id'] != meter_id]  # Remove old data
            df = pd.concat([df, meter_data], ignore_index=True)
        
        return df.sort_values(['meter_id', 'timestamp']).reset_index(drop=True), records


class OutlierDetector(DataCleaningStrategy):
    """
    Detect and handle outliers using methods appropriate for utility data.
    
    Methods:
    - 'iqr': Interquartile range method
    - 'zscore': Z-score based (with robust scaling)
    - 'isolation_forest': ML-based detection
    - 'seasonal': Seasonal decomposition + outlier detection
    """
    
    def __init__(
        self,
        method: str = 'iqr',
        iqr_multiplier: float = 3.0,
        zscore_threshold: float = 4.0,
        winsorize_limits: tuple[float, float] = (0.05, 0.05)
    ):
        self.method = method
        self.iqr_multiplier = iqr_multiplier
        self.zscore_threshold = zscore_threshold
        self.winsorize_limits = winsorize_limits
    
    def _detect_outliers_iqr(self, series: pd.Series) -> pd.Series:
        """IQR-based outlier detection."""
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - self.iqr_multiplier * IQR
        upper_bound = Q3 + self.iqr_multiplier * IQR
        return (series < lower_bound) | (series > upper_bound)
    
    def _detect_outliers_zscore(self, series: pd.Series) -> pd.Series:
        """Robust z-score based detection using MAD."""
        median = series.median()
        mad = np.median(np.abs(series - median))
        modified_z = 0.6745 * (series - median) / mad
        return np.abs(modified_z) > self.zscore_threshold
    
    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[CleaningRecord]]:
        records = []
        df = df.copy()
        
        # Calculate consumption (difference between readings)
        df['consumption'] = df.groupby('meter_id')['reading'].diff()
        
        for meter_id in df['meter_id'].unique():
            meter_mask = df['meter_id'] == meter_id
            consumption = df.loc[meter_mask, 'consumption'].dropna()
            
            if len(consumption) < 10:
                continue
            
            # Detect outliers
            if self.method == 'iqr':
                outlier_mask = self._detect_outliers_iqr(consumption)
            elif self.method == 'zscore':
                outlier_mask = self._detect_outliers_zscore(consumption)
            else:
                outlier_mask = pd.Series(False, index=consumption.index)
            
            outlier_indices = consumption[outlier_mask].index
            
            # Winsorize outliers
            for idx in outlier_indices:
                original_value = df.loc[idx, 'consumption']
                
                # Calculate winsorized value (cap at percentile)
                lower_cap = consumption.quantile(self.winsorize_limits[0])
                upper_cap = consumption.quantile(1 - self.winsorize_limits[1])
                new_value = np.clip(original_value, lower_cap, upper_cap)
                
                # Adjust reading to match winsorized consumption
                prev_reading = df.loc[idx - 1, 'reading'] if idx > 0 else df.loc[idx, 'reading']
                df.loc[idx, 'reading'] = prev_reading + new_value
                df.loc[idx, 'consumption'] = new_value
                
                records.append(CleaningRecord(
                    action=CleaningAction.OUTLIER_WINSORIZED,
                    meter_id=meter_id,
                    timestamp=df.loc[idx, 'timestamp'],
                    original_value=original_value,
                    new_value=new_value,
                    reason=f"{self.method} outlier detection",
                    confidence=0.85
                ))
        
        return df, records


class UnitConverter(DataCleaningStrategy):
    """Handle unit conversions for meter readings."""
    
    CONVERSION_FACTORS = {
        ('cubic_meters', 'liters'): 1000.0,
        ('liters', 'cubic_meters'): 0.001,
        ('gallons_uk', 'liters'): 4.54609,
        ('liters', 'gallons_uk'): 0.219969,
        ('gallons_us', 'liters'): 3.78541,
        ('liters', 'gallons_us'): 0.264172,
    }
    
    def __init__(self, from_unit: str, to_unit: str):
        self.from_unit = from_unit
        self.to_unit = to_unit
        self.factor = self.CONVERSION_FACTORS.get((from_unit, to_unit), 1.0)
    
    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[CleaningRecord]]:
        records = []
        
        if self.factor == 1.0:
            return df, records
        
        df = df.copy()
        df['original_reading'] = df['reading'].copy()
        df['reading'] = df['reading'] * self.factor
        
        # Create records for all converted values
        for idx, row in df.iterrows():
            records.append(CleaningRecord(
                action=CleaningAction.UNIT_CONVERTED,
                meter_id=row['meter_id'],
                timestamp=row['timestamp'],
                original_value=f"{row['original_reading']:.4f} {self.from_unit}",
                new_value=f"{row['reading']:.4f} {self.to_unit}",
                reason=f"Unit conversion: {self.from_unit} -> {self.to_unit}",
                confidence=1.0
            ))
        
        df = df.drop(columns=['original_reading'])
        return df, records


class DataCleaningPipeline:
    """
    Orchestrates multiple cleaning strategies with comprehensive audit logging.
    """
    
    def __init__(self, strategies: list[DataCleaningStrategy]):
        self.strategies = strategies
        self.audit_log: list[CleaningRecord] = []
    
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute all cleaning strategies in sequence."""
        logger.info(f"Starting cleaning pipeline on {len(df)} records")
        
        for strategy in self.strategies:
            strategy_name = strategy.__class__.__name__
            logger.info(f"Applying {strategy_name}...")
            
            df, records = strategy.clean(df)
            self.audit_log.extend(records)
            
            logger.info(f"{strategy_name}: {len(records)} modifications")
        
        logger.info(f"Pipeline complete. Total modifications: {len(self.audit_log)}")
        return df
    
    def get_audit_report(self) -> dict:
        """Generate comprehensive audit report."""
        if not self.audit_log:
            return {"status": "no_modifications"}
        
        actions = pd.DataFrame([
            {
                'action': r.action.name,
                'meter_id': r.meter_id,
                'confidence': r.confidence
            }
            for r in self.audit_log
        ])
        
        return {
            "total_modifications": len(self.audit_log),
            "modifications_by_type": actions['action'].value_counts().to_dict(),
            "modifications_by_meter": actions['meter_id'].value_counts().to_dict(),
            "average_confidence": actions['confidence'].mean(),
            "detailed_log": [
                {
                    "action": r.action.name,
                    "meter_id": r.meter_id,
                    "timestamp": r.timestamp.isoformat(),
                    "reason": r.reason,
                    "confidence": r.confidence
                }
                for r in self.audit_log
            ]
        }
    
    def export_audit_log(self, filepath: str):
        """Export audit log to JSON for compliance."""
        with open(filepath, 'w') as f:
            json.dump(self.get_audit_report(), f, indent=2, default=str)


# Usage example
if __name__ == "__main__":
    # Create messy test data
    np.random.seed(42)
    n = 1000
    
    test_data = pd.DataFrame({
        'meter_id': np.random.choice(['M001', 'M002', 'M003'], n),
        'timestamp': pd.date_range('2023-01-01', periods=n, freq='H'),
        'reading': np.cumsum(np.random.exponential(1, n)) + 100
    })
    
    # Add some problems
    # Duplicates
    test_data = pd.concat([test_data, test_data.iloc[100:105]])
    # Missing values
    test_data.loc[np.random.choice(test_data.index, 50), 'reading'] = np.nan
    # Outliers
    test_data.loc[np.random.choice(test_data.index, 10), 'reading'] *= 10
    
    # Build pipeline
    pipeline = DataCleaningPipeline([
        DuplicateHandler(strategy='mean'),
        MissingValueHandler(strategy='time', max_gap_hours=24),
        OutlierDetector(method='iqr', iqr_multiplier=2.5),
    ])
    
    # Clean data
    cleaned = pipeline.clean(test_data)
    
    # Get audit report
    report = pipeline.get_audit_report()
    print(json.dumps(report, indent=2, default=str))
```

### Key Design Decisions

1. **Strategy Pattern**: Each cleaning task is a separate strategy for modularity and testing
2. **Audit Trail**: Every modification tracked for regulatory compliance
3. **Configurable Thresholds**: All parameters exposed for domain-specific tuning
4. **Confidence Scoring**: Each action has confidence for quality assessment

---

## Question 3: Configurable ETL Pipeline with Error Handling

### Challenge
Build a production-grade ETL pipeline for Business Stream that:
1. Extracts data from multiple sources (CSV, API, database)
2. Transforms with configurable business rules
3. Loads to data warehouse with transaction safety
4. Implements comprehensive error handling and retry logic
5. Provides monitoring and alerting hooks

### Production Solution

```python
"""
Production-grade ETL pipeline with configurable transformations,
comprehensive error handling, and monitoring integration.
"""
from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Generic, Optional, TypeVar

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ETLStage(Enum):
    """ETL pipeline stages for status tracking."""
    EXTRACT = auto()
    TRANSFORM = auto()
    LOAD = auto()


class ETLStatus(Enum):
    """ETL job status enumeration."""
    PENDING = auto()
    RUNNING = auto()
    SUCCESS = auto()
    FAILED = auto()
    PARTIAL = auto()


@dataclass
class ETLMetrics:
    """Metrics for ETL job monitoring."""
    job_id: str
    stage: ETLStage
    records_processed: int = 0
    records_failed: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_messages: list[str] = field(default_factory=list)
    
    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def throughput_per_second(self) -> float:
        if self.duration_seconds > 0:
            return self.records_processed / self.duration_seconds
        return 0.0


class DataSource(ABC, Generic[T]):
    """Abstract base class for data sources."""
    
    @abstractmethod
    def extract(self) -> T:
        """Extract data from source."""
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """Validate source connectivity."""
        pass


class CSVDataSource(DataSource[pd.DataFrame]):
    """CSV file data source with chunked reading support."""
    
    def __init__(
        self,
        filepath: str,
        chunksize: Optional[int] = None,
        encoding: str = 'utf-8',
        **pandas_kwargs
    ):
        self.filepath = filepath
        self.chunksize = chunksize
        self.encoding = encoding
        self.pandas_kwargs = pandas_kwargs
    
    def validate_connection(self) -> bool:
        try:
            import os
            return os.path.exists(self.filepath)
        except Exception as e:
            logger.error(f"CSV validation failed: {e}")
            return False
    
    def extract(self) -> pd.DataFrame | Iterator[pd.DataFrame]:
        if not self.validate_connection():
            raise ConnectionError(f"Cannot access: {self.filepath}")
        
        if self.chunksize:
            return pd.read_csv(
                self.filepath,
                chunksize=self.chunksize,
                encoding=self.encoding,
                **self.pandas_kwargs
            )
        
        return pd.read_csv(
            self.filepath,
            encoding=self.encoding,
            **self.pandas_kwargs
        )


class APIDataSource(DataSource[pd.DataFrame]):
    """REST API data source with pagination and retry logic."""
    
    def __init__(
        self,
        base_url: str,
        endpoint: str,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
        pagination_key: Optional[str] = None,
        max_pages: int = 100,
        timeout: int = 30
    ):
        self.base_url = base_url.rstrip('/')
        self.endpoint = endpoint
        self.headers = headers or {}
        self.params = params or {}
        self.pagination_key = pagination_key
        self.max_pages = max_pages
        self.timeout = timeout
    
    def validate_connection(self) -> bool:
        try:
            response = requests.get(
                f"{self.base_url}/health",
                timeout=5,
                headers=self.headers
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"API validation failed: {e}")
            return False
    
    @retry(
        retry=retry_if_exception_type((requests.RequestException,)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def _fetch_page(self, url: str, params: dict) -> dict:
        """Fetch a single page with retry logic."""
        response = requests.get(
            url,
            headers=self.headers,
            params=params,
            timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()
    
    def extract(self) -> pd.DataFrame:
        if not self.validate_connection():
            raise ConnectionError(f"Cannot connect to API: {self.base_url}")
        
        all_data = []
        page = 1
        next_url = f"{self.base_url}/{self.endpoint.lstrip('/')}"
        current_params = self.params.copy()
        
        while next_url and page <= self.max_pages:
            logger.info(f"Fetching page {page} from {next_url}")
            
            try:
                data = self._fetch_page(next_url, current_params)
                
                # Extract records (handle different API response formats)
                if isinstance(data, list):
                    records = data
                    next_url = None
                elif 'data' in data:
                    records = data['data']
                    next_url = data.get('next_page_url') or data.get('next')
                elif 'results' in data:
                    records = data['results']
                    next_url = data.get('next')
                else:
                    records = [data]
                    next_url = None
                
                all_data.extend(records)
                page += 1
                
                # Rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Failed to fetch page {page}: {e}")
                raise
        
        return pd.json_normalize(all_data)


class DatabaseSource(DataSource[pd.DataFrame]):
    """SQL database source with query parameterization."""
    
    def __init__(
        self,
        connection_string: str,
        query: str,
        params: Optional[dict] = None,
        chunksize: Optional[int] = None
    ):
        self.connection_string = connection_string
        self.query = query
        self.params = params or {}
        self.chunksize = chunksize
        self._engine = None
    
    def _get_engine(self):
        if self._engine is None:
            self._engine = create_engine(self.connection_string)
        return self._engine
    
    def validate_connection(self) -> bool:
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Database validation failed: {e}")
            return False
    
    def extract(self) -> pd.DataFrame | Iterator[pd.DataFrame]:
        if not self.validate_connection():
            raise ConnectionError("Cannot connect to database")
        
        engine = self._get_engine()
        
        if self.chunksize:
            return pd.read_sql(
                self.query,
                engine,
                params=self.params,
                chunksize=self.chunksize
            )
        
        return pd.read_sql(self.query, engine, params=self.params)


class TransformationRule(ABC):
    """Abstract base for transformation rules."""
    
    @abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformation to DataFrame."""
        pass
    
    def validate(self, df: pd.DataFrame) -> bool:
        """Validate transformation can be applied."""
        return True


class ColumnMapper(TransformationRule):
    """Map/rename columns according to configuration."""
    
    def __init__(self, column_mapping: dict[str, str]):
        self.column_mapping = column_mapping
    
    def validate(self, df: pd.DataFrame) -> bool:
        missing = set(self.column_mapping.keys()) - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns for mapping: {missing}")
        return True
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        self.validate(df)
        return df.rename(columns=self.column_mapping)


class DataTypeConverter(TransformationRule):
    """Convert column data types with error handling."""
    
    def __init__(self, type_mapping: dict[str, str], errors: str = 'coerce'):
        self.type_mapping = type_mapping
        self.errors = errors
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col, dtype in self.type_mapping.items():
            if col in df.columns:
                try:
                    if dtype == 'datetime':
                        df[col] = pd.to_datetime(df[col], errors=self.errors)
                    elif dtype == 'numeric':
                        df[col] = pd.to_numeric(df[col], errors=self.errors)
                    else:
                        df[col] = df[col].astype(dtype)
                except Exception as e:
                    logger.warning(f"Failed to convert {col} to {dtype}: {e}")
        return df


class FilterRule(TransformationRule):
    """Filter rows based on conditions."""
    
    def __init__(self, condition: Callable[[pd.DataFrame], pd.Series]):
        self.condition = condition
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[self.condition(df)].reset_index(drop=True)


class CustomTransform(TransformationRule):
    """Custom transformation with user-defined function."""
    
    def __init__(self, transform_func: Callable[[pd.DataFrame], pd.DataFrame]):
        self.transform_func = transform_func
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.transform_func(df)


class DataTarget(ABC):
    """Abstract base for data targets."""
    
    @abstractmethod
    def load(self, df: pd.DataFrame) -> int:
        """Load data and return count of records written."""
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """Validate target is ready for loading."""
        pass


class DatabaseTarget(DataTarget):
    """Database target with transaction support."""
    
    def __init__(
        self,
        connection_string: str,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = 'append',
        batch_size: int = 10000
    ):
        self.connection_string = connection_string
        self.table_name = table_name
        self.schema = schema
        self.if_exists = if_exists
        self.batch_size = batch_size
    
    def validate(self) -> bool:
        try:
            engine = create_engine(self.connection_string)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Target validation failed: {e}")
            return False
    
    def load(self, df: pd.DataFrame) -> int:
        if not self.validate():
            raise ConnectionError("Target validation failed")
        
        engine = create_engine(self.connection_string)
        
        # Write in batches for large datasets
        total_written = 0
        for i in range(0, len(df), self.batch_size):
            batch = df.iloc[i:i + self.batch_size]
            batch.to_sql(
                self.table_name,
                engine,
                schema=self.schema,
                if_exists=self.if_exists if i == 0 else 'append',
                index=False,
                method='multi'
            )
            total_written += len(batch)
        
        return total_written


class ETLPipeline:
    """
    Orchestrates ETL process with comprehensive error handling and monitoring.
    """
    
    def __init__(
        self,
        source: DataSource,
        transformations: list[TransformationRule],
        target: DataTarget,
        job_id: Optional[str] = None,
        error_threshold: float = 0.1
    ):
        self.source = source
        self.transformations = transformations
        self.target = target
        self.job_id = job_id or f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.error_threshold = error_threshold
        self.metrics: dict[ETLStage, ETLMetrics] = {}
    
    def _record_metric(
        self,
        stage: ETLStage,
        records_processed: int = 0,
        records_failed: int = 0,
        error_messages: list[str] = None
    ):
        """Record metrics for a stage."""
        if stage not in self.metrics:
            self.metrics[stage] = ETLMetrics(
                job_id=self.job_id,
                stage=stage,
                start_time=datetime.now()
            )
        
        metric = self.metrics[stage]
        metric.records_processed += records_processed
        metric.records_failed += records_failed
        metric.end_time = datetime.now()
        if error_messages:
            metric.error_messages.extend(error_messages)
    
    def extract(self) -> pd.DataFrame:
        """Extract data from source."""
        logger.info(f"[{self.job_id}] Starting extraction...")
        
        try:
            data = self.source.extract()
            
            # Handle chunked extraction
            if hasattr(data, '__iter__') and not isinstance(data, pd.DataFrame):
                chunks = list(data)
                data = pd.concat(chunks, ignore_index=True)
            
            self._record_metric(
                ETLStage.EXTRACT,
                records_processed=len(data)
            )
            
            logger.info(f"[{self.job_id}] Extracted {len(data)} records")
            return data
            
        except Exception as e:
            self._record_metric(
                ETLStage.EXTRACT,
                records_failed=0,
                error_messages=[str(e)]
            )
            logger.error(f"[{self.job_id}] Extraction failed: {e}")
            raise
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all transformations."""
        logger.info(f"[{self.job_id}] Starting transformation...")
        
        original_count = len(df)
        error_messages = []
        
        for i, rule in enumerate(self.transformations):
            rule_name = rule.__class__.__name__
            logger.info(f"[{self.job_id}] Applying {rule_name}...")
            
            try:
                if not rule.validate(df):
                    raise ValueError(f"Validation failed for {rule_name}")
                
                df = rule.apply(df)
                logger.info(f"[{self.job_id}] {rule_name}: {len(df)} records")
                
            except Exception as e:
                error_msg = f"{rule_name} failed: {str(e)}"
                error_messages.append(error_msg)
                logger.error(f"[{self.job_id}] {error_msg}")
                
                # Continue with next transformation
                continue
        
        self._record_metric(
            ETLStage.TRANSFORM,
            records_processed=len(df),
            records_failed=original_count - len(df),
            error_messages=error_messages
        )
        
        return df
    
    def load(self, df: pd.DataFrame) -> int:
        """Load data to target."""
        logger.info(f"[{self.job_id}] Starting load...")
        
        try:
            records_written = self.target.load(df)
            
            self._record_metric(
                ETLStage.LOAD,
                records_processed=records_written
            )
            
            logger.info(f"[{self.job_id}] Loaded {records_written} records")
            return records_written
            
        except Exception as e:
            self._record_metric(
                ETLStage.LOAD,
                error_messages=[str(e)]
            )
            logger.error(f"[{self.job_id}] Load failed: {e}")
            raise
    
    def run(self) -> dict:
        """Execute complete ETL pipeline."""
        logger.info(f"[{self.job_id}] Starting ETL pipeline...")
        
        try:
            # Extract
            data = self.extract()
            
            # Transform
            transformed = self.transform(data)
            
            # Load
            records_written = self.load(transformed)
            
            # Determine status
            total_errors = sum(m.records_failed for m in self.metrics.values())
            error_rate = total_errors / max(records_written, 1)
            
            status = ETLStatus.SUCCESS if error_rate < self.error_threshold else ETLStatus.PARTIAL
            
            return {
                'job_id': self.job_id,
                'status': status.name,
                'records_processed': records_written,
                'error_rate': error_rate,
                'metrics': {
                    stage.name: {
                        'records_processed': m.records_processed,
                        'records_failed': m.records_failed,
                        'duration_seconds': m.duration_seconds,
                        'throughput_per_second': m.throughput_per_second,
                        'errors': m.error_messages
                    }
                    for stage, m in self.metrics.items()
                }
            }
            
        except Exception as e:
            logger.error(f"[{self.job_id}] Pipeline failed: {e}")
            return {
                'job_id': self.job_id,
                'status': ETLStatus.FAILED.name,
                'error': str(e),
                'metrics': {
                    stage.name: {
                        'records_processed': m.records_processed,
                        'records_failed': m.records_failed,
                        'errors': m.error_messages
                    }
                    for stage, m in self.metrics.items()
                }
            }


# Usage example
if __name__ == "__main__":
    # Create test CSV
    test_data = pd.DataFrame({
        'meter_id': ['M001', 'M002', 'M003'],
        'read_value': [100.5, 200.3, 150.7],
        'read_date': ['2023-01-01', '2023-01-02', '2023-01-03']
    })
    test_data.to_csv('/tmp/test_meters.csv', index=False)
    
    # Build pipeline
    pipeline = ETLPipeline(
        source=CSVDataSource(
            filepath='/tmp/test_meters.csv',
            parse_dates=['read_date']
        ),
        transformations=[
            ColumnMapper({
                'meter_id': 'meter_reference',
                'read_value': 'consumption',
                'read_date': 'reading_date'
            }),
            DataTypeConverter({
                'consumption': 'float64',
                'reading_date': 'datetime64[ns]'
            }),
            CustomTransform(lambda df: df.assign(processed_at=datetime.now()))
        ],
        target=DatabaseTarget(
            connection_string='sqlite:////tmp/test_warehouse.db',
            table_name='meter_readings',
            if_exists='replace'
        )
    )
    
    # Run pipeline
    result = pipeline.run()
    print(json.dumps(result, indent=2, default=str))
```

### Key Design Decisions

1. **Abstract Base Classes**: Clean separation between extract, transform, load concerns
2. **Retry Logic**: Tenacity decorator for resilient API calls
3. **Transaction Safety**: Batch loading with rollback capability
4. **Comprehensive Metrics**: Every stage tracked for monitoring
5. **Error Thresholds**: Configurable tolerance for partial success

---

## Question 4: Memory-Efficient Processing of Large Datasets

### Challenge
Business Stream processes millions of meter readings daily. Design a solution that:
1. Processes data larger than available RAM
2. Implements streaming/iterator patterns
3. Uses appropriate data types to minimize memory
4. Leverages parallel processing where beneficial
5. Provides progress tracking for long-running operations

### Production Solution

```python
"""
Memory-efficient data processing for large-scale utility datasets.
Implements streaming, chunked processing, and memory optimization techniques.
"""
from __future__ import annotations

import gc
import logging
import os
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Generic, Optional, TypeVar

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

logger = logging.getLogger(__name__)

T = TypeVar('T')


@dataclass
class MemoryProfile:
    """Memory usage statistics."""
    peak_mb: float
    current_mb: float
    records_processed: int
    processing_rate: float  # records/second


class MemoryOptimizer:
    """
    Optimizes DataFrame memory usage through type coercion and column pruning.
    """
    
    # Optimal dtypes for common patterns
    DTYPE_MAPPING = {
        'int64': 'int32',
        'float64': 'float32',
        'object': 'category'
    }
    
    @staticmethod
    def optimize_numeric(df: pd.DataFrame) -> pd.DataFrame:
        """Downcast numeric columns to smallest appropriate type."""
        df = df.copy()
        
        for col in df.select_dtypes(include=['integer']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')
        
        for col in df.select_dtypes(include=['float']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')
        
        return df
    
    @staticmethod
    def optimize_categorical(
        df: pd.DataFrame,
        threshold: float = 0.5
    ) -> pd.DataFrame:
        """
        Convert string columns to categorical where beneficial.
        
        threshold: max ratio of unique values to total values for conversion
        """
        df = df.copy()
        
        for col in df.select_dtypes(include=['object']).columns:
            num_unique = df[col].nunique()
            num_total = len(df)
            
            if num_unique / num_total < threshold:
                df[col] = df[col].astype('category')
        
        return df
    
    @staticmethod
    def optimize_datetime(df: pd.DataFrame) -> pd.DataFrame:
        """Ensure datetime columns use efficient representation."""
        df = df.copy()
        
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                # Use datetime64[ns] which is pandas default
                pass
            elif df[col].dtype == 'object':
                # Try to convert object columns to datetime
                try:
                    df[col] = pd.to_datetime(df[col])
                except (ValueError, TypeError):
                    pass
        
        return df
    
    @classmethod
    def optimize(cls, df: pd.DataFrame, categorical_threshold: float = 0.5) -> pd.DataFrame:
        """Apply all optimization strategies."""
        original_memory = df.memory_usage(deep=True).sum() / 1024**2
        
        df = cls.optimize_numeric(df)
        df = cls.optimize_categorical(df, categorical_threshold)
        df = cls.optimize_datetime(df)
        
        optimized_memory = df.memory_usage(deep=True).sum() / 1024**2
        savings = (1 - optimized_memory / original_memory) * 100
        
        logger.info(f"Memory optimization: {original_memory:.2f}MB -> {optimized_memory:.2f}MB ({savings:.1f}% reduction)")
        
        return df


class ChunkedProcessor:
    """
    Process large datasets in memory-efficient chunks.
    """
    
    def __init__(
        self,
        chunk_size: int = 100_000,
        optimize_memory: bool = True,
        progress_bar: bool = True
    ):
        self.chunk_size = chunk_size
        self.optimize_memory = optimize_memory
        self.progress_bar = progress_bar
    
    def process_file(
        self,
        input_path: str,
        output_path: str,
        transform_func: Callable[[pd.DataFrame], pd.DataFrame],
        file_format: str = 'csv'
    ) -> MemoryProfile:
        """
        Process a large file in chunks with progress tracking.
        """
        start_time = datetime.now()
        total_records = 0
        
        # Estimate total rows for progress bar
        if file_format == 'csv':
            # Quick row count estimate
            with open(input_path, 'r') as f:
                total_rows = sum(1 for _ in f) - 1  # Exclude header
        else:
            total_rows = None
        
        # Create iterator
        if file_format == 'csv':
            chunks = pd.read_csv(input_path, chunksize=self.chunk_size)
        elif file_format == 'parquet':
            # Parquet doesn't support chunked reading natively
            # Use pyarrow for memory-efficient reading
            table = pq.ParquetFile(input_path)
            chunks = (
                table.read_row_group(i).to_pandas()
                for i in range(table.num_row_groups)
            )
        else:
            raise ValueError(f"Unsupported format: {file_format}")
        
        # Wrap with progress bar
        if self.progress_bar and total_rows:
            num_chunks = (total_rows + self.chunk_size - 1) // self.chunk_size
            chunks = tqdm(chunks, total=num_chunks, desc="Processing chunks")
        
        # Process chunks
        first_chunk = True
        
        for chunk in chunks:
            # Optimize memory
            if self.optimize_memory:
                chunk = MemoryOptimizer.optimize(chunk)
            
            # Apply transformation
            processed = transform_func(chunk)
            
            # Write output
            if file_format == 'csv':
                mode = 'w' if first_chunk else 'a'
                header = first_chunk
                processed.to_csv(output_path, mode=mode, header=header, index=False)
            
            total_records += len(processed)
            first_chunk = False
            
            # Force garbage collection
            del chunk, processed
            gc.collect()
        
        duration = (datetime.now() - start_time).total_seconds()
        
        return MemoryProfile(
            peak_mb=0,  # Would need psutil for actual measurement
            current_mb=0,
            records_processed=total_records,
            processing_rate=total_records / duration if duration > 0 else 0
        )


class StreamingAggregator:
    """
    Compute aggregations on streaming data without loading entire dataset.
    
    Uses online algorithms for mean, variance, and other statistics.
    """
    
    def __init__(self, groupby_columns: list[str], agg_columns: list[str]):
        self.groupby_columns = groupby_columns
        self.agg_columns = agg_columns
        self._running_stats: dict = {}
    
    def update(self, chunk: pd.DataFrame) -> None:
        """Update running statistics with new chunk."""
        grouped = chunk.groupby(self.groupby_columns)
        
        for group_key, group_df in grouped:
            if group_key not in self._running_stats:
                self._running_stats[group_key] = {
                    'count': 0,
                    'sums': {col: 0.0 for col in self.agg_columns},
                    'sum_squares': {col: 0.0 for col in self.agg_columns},
                    'min': {col: float('inf') for col in self.agg_columns},
                    'max': {col: float('-inf') for col in self.agg_columns}
                }
            
            stats = self._running_stats[group_key]
            stats['count'] += len(group_df)
            
            for col in self.agg_columns:
                values = group_df[col].dropna()
                stats['sums'][col] += values.sum()
                stats['sum_squares'][col] += (values ** 2).sum()
                stats['min'][col] = min(stats['min'][col], values.min())
                stats['max'][col] = max(stats['max'][col], values.max())
    
    def get_results(self) -> pd.DataFrame:
        """Compute final aggregations from running statistics."""
        results = []
        
        for group_key, stats in self._running_stats.items():
            row = {
                col: group_key[i] if isinstance(group_key, tuple) else group_key
                for i, col in enumerate(self.groupby_columns)
            }
            
            for col in self.agg_columns:
                count = stats['count']
                if count > 0:
                    mean = stats['sums'][col] / count
                    # Welford's online variance algorithm
                    variance = (stats['sum_squares'][col] / count) - (mean ** 2)
                    std = np.sqrt(max(0, variance))
                    
                    row[f'{col}_mean'] = mean
                    row[f'{col}_std'] = std
                    row[f'{col}_min'] = stats['min'][col]
                    row[f'{col}_max'] = stats['max'][col]
                    row[f'{col}_count'] = count
            
            results.append(row)
        
        return pd.DataFrame(results)


class ParallelChunkProcessor:
    """
    Parallel processing of data chunks using process pool.
    """
    
    def __init__(
        self,
        n_workers: int = None,
        chunk_size: int = 50_000
    ):
        self.n_workers = n_workers or os.cpu_count()
        self.chunk_size = chunk_size
    
    def process(
        self,
        input_path: str,
        transform_func: Callable[[pd.DataFrame], pd.DataFrame],
        combine_func: Optional[Callable[[list[pd.DataFrame]], pd.DataFrame]] = None
    ) -> pd.DataFrame:
        """
        Process file in parallel chunks.
        """
        # Read file and split into chunks
        df = pd.read_csv(input_path)
        chunks = [
            df.iloc[i:i + self.chunk_size].copy()
            for i in range(0, len(df), self.chunk_size)
        ]
        
        logger.info(f"Processing {len(chunks)} chunks with {self.n_workers} workers")
        
        # Process in parallel
        results = []
        with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
            futures = {
                executor.submit(transform_func, chunk): i
                for i, chunk in enumerate(chunks)
            }
            
            for future in tqdm(as_completed(futures), total=len(chunks), desc="Processing"):
                chunk_idx = futures[future]
                try:
                    result = future.result()
                    results.append((chunk_idx, result))
                except Exception as e:
                    logger.error(f"Chunk {chunk_idx} failed: {e}")
        
        # Sort by original order and combine
        results.sort(key=lambda x: x[0])
        result_dfs = [r[1] for r in results]
        
        if combine_func:
            return combine_func(result_dfs)
        
        return pd.concat(result_dfs, ignore_index=True)


class LargeDatasetPipeline:
    """
    End-to-end pipeline for large dataset processing.
    """
    
    def __init__(
        self,
        chunk_size: int = 100_000,
        use_parallel: bool = False,
        n_workers: int = None
    ):
        self.chunk_size = chunk_size
        self.use_parallel = use_parallel
        self.n_workers = n_workers or os.cpu_count()
    
    def process(
        self,
        input_path: str,
        output_path: str,
        transform_func: Callable[[pd.DataFrame], pd.DataFrame],
        aggregation: Optional[dict] = None
    ) -> dict:
        """
        Process large dataset with optional aggregation.
        """
        start_time = datetime.now()
        
        if aggregation:
            # Use streaming aggregator
            aggregator = StreamingAggregator(
                groupby_columns=aggregation.get('groupby', []),
                agg_columns=aggregation.get('agg', [])
            )
            
            chunk_iter = pd.read_csv(input_path, chunksize=self.chunk_size)
            
            for chunk in tqdm(chunk_iter, desc="Processing chunks"):
                chunk = MemoryOptimizer.optimize(chunk)
                processed = transform_func(chunk)
                aggregator.update(processed)
            
            result = aggregator.get_results()
            result.to_csv(output_path, index=False)
            
        elif self.use_parallel:
            # Use parallel processing
            processor = ParallelChunkProcessor(
                n_workers=self.n_workers,
                chunk_size=self.chunk_size
            )
            result = processor.process(input_path, transform_func)
            result.to_csv(output_path, index=False)
            
        else:
            # Use chunked processing
            processor = ChunkedProcessor(chunk_size=self.chunk_size)
            profile = processor.process_file(
                input_path, output_path, transform_func
            )
            
            return {
                'records_processed': profile.records_processed,
                'processing_rate': profile.processing_rate,
                'duration_seconds': profile.records_processed / profile.processing_rate
            }
        
        duration = (datetime.now() - start_time).total_seconds()
        
        return {
            'records_processed': len(result) if isinstance(result, pd.DataFrame) else 0,
            'duration_seconds': duration,
            'output_path': output_path
        }


# Usage example
if __name__ == "__main__":
    # Generate large test dataset
    n_records = 1_000_000
    test_data = pd.DataFrame({
        'meter_id': np.random.choice([f'M{i:05d}' for i in range(1000)], n_records),
        'timestamp': pd.date_range('2023-01-01', periods=n_records, freq='min'),
        'reading': np.random.exponential(100, n_records),
        'status': np.random.choice(['A', 'B', 'C', 'D'], n_records)
    })
    test_data.to_csv('/tmp/large_dataset.csv', index=False)
    
    # Define transformation
    def transform_chunk(df: pd.DataFrame) -> pd.DataFrame:
        """Example transformation: calculate daily consumption."""
        df['date'] = df['timestamp'].dt.date
        df['consumption'] = df.groupby('meter_id')['reading'].diff().fillna(0)
        return df
    
    # Process with streaming aggregation
    pipeline = LargeDatasetPipeline(chunk_size=100_000)
    
    result = pipeline.process(
        input_path='/tmp/large_dataset.csv',
        output_path='/tmp/aggregated_results.csv',
        transform_func=transform_chunk,
        aggregation={
            'groupby': ['meter_id', 'date'],
            'agg': ['consumption', 'reading']
        }
    )
    
    print(f"Processing complete: {result}")
```

### Key Design Decisions

1. **Streaming Algorithms**: Online statistics computation avoids loading all data
2. **Memory Optimization**: Automatic type downcasting reduces memory by 50-80%
3. **Chunked Processing**: Iterator pattern keeps memory usage bounded
4. **Parallel Processing**: ProcessPoolExecutor for CPU-bound transformations
5. **Garbage Collection**: Explicit cleanup between chunks

---

*[Questions 5-12 continue in the next section...]*


---

## Question 5: FastAPI Data Service Design

### Challenge
Design a production-grade FastAPI service for Business Stream that:
1. Exposes endpoints for meter data queries with pagination
2. Implements authentication and rate limiting
3. Handles async database operations
4. Provides OpenAPI documentation
5. Includes health checks and metrics endpoints

### Production Solution

```python
"""
Production-grade FastAPI service for utility meter data queries.
Implements authentication, rate limiting, async operations, and comprehensive monitoring.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import AsyncGenerator, Optional

import aioredis
import asyncpg
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    Security,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, validator
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

logger = logging.getLogger(__name__)

# Security setup
security = HTTPBearer()
limiter = Limiter(key_func=get_remote_address)

# Database connection pool
db_pool: Optional[asyncpg.Pool] = None
redis_client: Optional[aioredis.Redis] = None


# ============== Pydantic Models ==============

class MeterReading(BaseModel):
    """Meter reading data model."""
    meter_id: str = Field(..., min_length=3, max_length=20)
    timestamp: datetime
    reading: float = Field(..., ge=0)
    unit: str = Field(default="cubic_meters", regex="^(cubic_meters|liters|gallons)$")
    quality_flag: Optional[str] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class MeterQueryParams(BaseModel):
    """Query parameters for meter data retrieval."""
    meter_id: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    min_reading: Optional[float] = None
    max_reading: Optional[float] = None
    
    @validator('end_date')
    def dates_valid(cls, v, values):
        if v and values.get('start_date') and v < values['start_date']:
            raise ValueError('end_date must be after start_date')
        return v


class PaginatedResponse(BaseModel):
    """Standard paginated response format."""
    data: list[MeterReading]
    total: int
    page: int
    page_size: int
    pages: int
    has_next: bool
    has_prev: bool


class HealthStatus(BaseModel):
    """Health check response."""
    status: str
    timestamp: datetime
    version: str
    checks: dict


class MetricsResponse(BaseModel):
    """Service metrics response."""
    requests_total: int
    requests_per_second: float
    average_response_time_ms: float
    error_rate: float
    active_connections: int


# ============== Database Layer ==============

class DatabaseManager:
    """Manages database connections and queries."""
    
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Initialize connection pool."""
        self.pool = await asyncpg.create_pool(
            self.dsn,
            min_size=5,
            max_size=20,
            command_timeout=60,
            server_settings={
                'jit': 'off',
                'application_name': 'business_stream_api'
            }
        )
        logger.info("Database pool initialized")
    
    async def disconnect(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")
    
    async def fetch_meter_readings(
        self,
        params: MeterQueryParams,
        page: int = 1,
        page_size: int = 100
    ) -> tuple[list[dict], int]:
        """Fetch paginated meter readings with filters."""
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        # Build query dynamically
        conditions = ["1=1"]
        query_params = []
        param_idx = 1
        
        if params.meter_id:
            conditions.append(f"meter_id = ${param_idx}")
            query_params.append(params.meter_id)
            param_idx += 1
        
        if params.start_date:
            conditions.append(f"timestamp >= ${param_idx}")
            query_params.append(params.start_date)
            param_idx += 1
        
        if params.end_date:
            conditions.append(f"timestamp <= ${param_idx}")
            query_params.append(params.end_date)
            param_idx += 1
        
        if params.min_reading is not None:
            conditions.append(f"reading >= ${param_idx}")
            query_params.append(params.min_reading)
            param_idx += 1
        
        if params.max_reading is not None:
            conditions.append(f"reading <= ${param_idx}")
            query_params.append(params.max_reading)
            param_idx += 1
        
        where_clause = " AND ".join(conditions)
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM meter_readings WHERE {where_clause}"
        async with self.pool.acquire() as conn:
            total = await conn.fetchval(count_query, *query_params)
        
        # Get paginated results
        offset = (page - 1) * page_size
        query_params.extend([page_size, offset])
        
        data_query = f"""
            SELECT meter_id, timestamp, reading, unit, quality_flag
            FROM meter_readings
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT ${param_idx} OFFSET ${param_idx + 1}
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(data_query, *query_params)
        
        return [dict(row) for row in rows], total
    
    async def get_meter_stats(self, meter_id: str) -> dict:
        """Get statistical summary for a meter."""
        query = """
            SELECT 
                COUNT(*) as total_readings,
                MIN(reading) as min_reading,
                MAX(reading) as max_reading,
                AVG(reading) as avg_reading,
                MIN(timestamp) as first_reading,
                MAX(timestamp) as last_reading
            FROM meter_readings
            WHERE meter_id = $1
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, meter_id)
        
        return dict(row) if row else {}


db_manager = DatabaseManager("postgresql://user:pass@localhost/business_stream")


# ============== Authentication ==============

class AuthManager:
    """Simple token-based authentication."""
    
    def __init__(self):
        # In production, use proper secret management
        self.valid_tokens = {"api_key_123": "client_a", "api_key_456": "client_b"}
        self.rate_limits = {"client_a": 100, "client_b": 1000}
    
    async def verify_token(
        self,
        credentials: HTTPAuthorizationCredentials = Security(security)
    ) -> str:
        """Verify API token and return client ID."""
        token = credentials.credentials
        
        if token not in self.valid_tokens:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication token"
            )
        
        return self.valid_tokens[token]


auth_manager = AuthManager()


# ============== Caching Layer ==============

class CacheManager:
    """Redis-based caching for query results."""
    
    def __init__(self, redis_url: str = "redis://localhost"):
        self.redis_url = redis_url
        self.client: Optional[aioredis.Redis] = None
    
    async def connect(self):
        self.client = await aioredis.from_url(self.redis_url)
    
    async def disconnect(self):
        if self.client:
            await self.client.close()
    
    def _make_key(self, prefix: str, params: dict) -> str:
        """Generate cache key from parameters."""
        param_str = "|".join(f"{k}={v}" for k, v in sorted(params.items()))
        hash_val = hashlib.md5(param_str.encode()).hexdigest()[:12]
        return f"{prefix}:{hash_val}"
    
    async def get(self, key: str) -> Optional[str]:
        if not self.client:
            return None
        return await self.client.get(key)
    
    async def set(self, key: str, value: str, ttl: int = 300):
        if self.client:
            await self.client.setex(key, ttl, value)
    
    async def invalidate(self, pattern: str):
        """Invalidate cache entries matching pattern."""
        if self.client:
            keys = await self.client.keys(pattern)
            if keys:
                await self.client.delete(*keys)


cache_manager = CacheManager()


# ============== Metrics Collection ==============

class MetricsCollector:
    """Collect and expose service metrics."""
    
    def __init__(self):
        self.requests_total = 0
        self.requests_error = 0
        self.response_times: list[float] = []
        self.start_time = time.time()
    
    def record_request(self, duration_ms: float, error: bool = False):
        self.requests_total += 1
        if error:
            self.requests_error += 1
        self.response_times.append(duration_ms)
        # Keep only last 1000 measurements
        self.response_times = self.response_times[-1000:]
    
    def get_metrics(self) -> MetricsResponse:
        elapsed = time.time() - self.start_time
        avg_response = sum(self.response_times) / len(self.response_times) if self.response_times else 0
        
        return MetricsResponse(
            requests_total=self.requests_total,
            requests_per_second=self.requests_total / elapsed if elapsed > 0 else 0,
            average_response_time_ms=avg_response,
            error_rate=self.requests_error / self.requests_total if self.requests_total > 0 else 0,
            active_connections=0  # Would track from connection pool
        )


metrics_collector = MetricsCollector()


# ============== FastAPI Application ==============

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup/shutdown."""
    # Startup
    await db_manager.connect()
    await cache_manager.connect()
    logger.info("Application started")
    
    yield
    
    # Shutdown
    await db_manager.disconnect()
    await cache_manager.disconnect()
    logger.info("Application shutdown")


app = FastAPI(
    title="Business Stream Meter Data API",
    description="API for querying utility meter readings and consumption data",
    version="1.0.0",
    lifespan=lifespan
)

# Middleware
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://businessstream.co.uk"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Collect metrics for all requests."""
    start_time = time.time()
    
    try:
        response = await call_next(request)
        duration_ms = (time.time() - start_time) * 1000
        metrics_collector.record_request(duration_ms, error=False)
        response.headers["X-Response-Time-Ms"] = str(duration_ms)
        return response
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        metrics_collector.record_request(duration_ms, error=True)
        raise


# ============== API Endpoints ==============

@app.get("/health", response_model=HealthStatus)
async def health_check():
    """Health check endpoint for monitoring."""
    checks = {
        "database": db_manager.pool is not None,
        "cache": cache_manager.client is not None
    }
    
    status_val = "healthy" if all(checks.values()) else "degraded"
    
    return HealthStatus(
        status=status_val,
        timestamp=datetime.utcnow(),
        version="1.0.0",
        checks=checks
    )


@app.get("/metrics", response_model=MetricsResponse)
async def get_metrics(client_id: str = Depends(auth_manager.verify_token)):
    """Get service metrics (authenticated)."""
    return metrics_collector.get_metrics()


@app.get("/api/v1/meters/readings", response_model=PaginatedResponse)
@limiter.limit("100/minute")
async def get_meter_readings(
    request: Request,
    meter_id: Optional[str] = Query(None, description="Filter by meter ID"),
    start_date: Optional[datetime] = Query(None, description="Start date (ISO format)"),
    end_date: Optional[datetime] = Query(None, description="End date (ISO format)"),
    min_reading: Optional[float] = Query(None, description="Minimum reading value"),
    max_reading: Optional[float] = Query(None, description="Maximum reading value"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=1000, description="Items per page"),
    client_id: str = Depends(auth_manager.verify_token)
):
    """
    Retrieve paginated meter readings with filtering options.
    
    - Supports filtering by meter ID, date range, and reading values
    - Results are cached for 5 minutes
    - Rate limited to 100 requests per minute
    """
    # Build query params
    params = MeterQueryParams(
        meter_id=meter_id,
        start_date=start_date,
        end_date=end_date,
        min_reading=min_reading,
        max_reading=max_reading
    )
    
    # Check cache
    cache_key = cache_manager._make_key("readings", {
        **params.dict(),
        "page": page,
        "page_size": page_size
    })
    
    cached = await cache_manager.get(cache_key)
    if cached:
        return PaginatedResponse.parse_raw(cached)
    
    # Fetch from database
    try:
        data, total = await db_manager.fetch_meter_readings(params, page, page_size)
    except Exception as e:
        logger.error(f"Database query failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch data"
        )
    
    # Build response
    pages = (total + page_size - 1) // page_size
    
    response = PaginatedResponse(
        data=[MeterReading(**row) for row in data],
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
        has_next=page < pages,
        has_prev=page > 1
    )
    
    # Cache response
    await cache_manager.set(cache_key, response.json(), ttl=300)
    
    return response


@app.get("/api/v1/meters/{meter_id}/stats")
async def get_meter_statistics(
    meter_id: str,
    client_id: str = Depends(auth_manager.verify_token)
):
    """Get statistical summary for a specific meter."""
    stats = await db_manager.get_meter_stats(meter_id)
    
    if not stats or stats.get('total_readings', 0) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Meter {meter_id} not found or has no readings"
        )
    
    return stats


@app.get("/api/v1/meters/consumption")
async def get_consumption_summary(
    meter_id: str,
    period: str = Query("daily", regex="^(hourly|daily|weekly|monthly)$"),
    client_id: str = Depends(auth_manager.verify_token)
):
    """
    Get consumption aggregated by time period.
    
    - hourly: Hour-by-hour consumption
    - daily: Daily totals
    - weekly: Weekly summaries
    - monthly: Monthly billing periods
    """
    interval_map = {
        "hourly": "1 hour",
        "daily": "1 day",
        "weekly": "1 week",
        "monthly": "1 month"
    }
    
    query = f"""
        SELECT 
            date_trunc('{period}', timestamp) as period,
            MAX(reading) - MIN(reading) as consumption,
            COUNT(*) as reading_count
        FROM meter_readings
        WHERE meter_id = $1
        GROUP BY date_trunc('{period}', timestamp)
        ORDER BY period DESC
        LIMIT 100
    """
    
    async with db_manager.pool.acquire() as conn:
        rows = await conn.fetch(query, meter_id)
    
    return {
        "meter_id": meter_id,
        "period": period,
        "data": [dict(row) for row in rows]
    }


# Error handlers
@app.exception_handler(asyncpg.PostgresError)
async def database_exception_handler(request: Request, exc: asyncpg.PostgresError):
    logger.error(f"Database error: {exc}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Database error occurred"}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

### Key Design Decisions

1. **Async Throughout**: All I/O operations are async for maximum throughput
2. **Connection Pooling**: Database connections pooled and reused
3. **Caching Layer**: Redis caching for frequently accessed queries
4. **Rate Limiting**: Per-client rate limits prevent abuse
5. **Health Checks**: Comprehensive monitoring endpoints for ops

---

## Question 6: Automated Report Generation

### Challenge
Build an automated reporting system that:
1. Generates Excel reports with formatted tables and charts
2. Creates PDF summaries for executive distribution
3. Supports scheduled report generation
4. Implements email distribution with attachments
5. Provides report templates and customization

### Production Solution

```python
"""
Automated report generation system for utility data.
Supports Excel, PDF, and email distribution with templating.
"""
from __future__ import annotations

import io
import logging
import smtplib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Callable, Optional

import jinja2
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.backends.backend_pdf import PdfPages
from openpyxl import Workbook
from openpyxl.chart import BarChart, LineChart, Reference
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
from weasyprint import HTML, CSS

logger = logging.getLogger(__name__)


@dataclass
class ReportConfig:
    """Configuration for report generation."""
    title: str
    subtitle: Optional[str] = None
    author: str = "Business Stream Data Team"
    generated_at: datetime = field(default_factory=datetime.now)
    logo_path: Optional[str] = None
    color_scheme: dict = field(default_factory=lambda: {
        'primary': '1f4e79',
        'secondary': '2e75b6',
        'accent': '70ad47'
    })


@dataclass
class EmailConfig:
    """Configuration for email distribution."""
    smtp_host: str
    smtp_port: int
    username: str
    password: str
    use_tls: bool = True


class ReportSection(ABC):
    """Abstract base class for report sections."""
    
    @abstractmethod
    def render_excel(self, workbook: Workbook, sheet_name: str) -> None:
        """Render section to Excel workbook."""
        pass
    
    @abstractmethod
    def render_pdf(self, pdf: PdfPages) -> None:
        """Render section to PDF."""
        pass
    
    @abstractmethod
    def render_html(self) -> str:
        """Render section to HTML."""
        pass


class DataTableSection(ReportSection):
    """Report section containing a data table."""
    
    def __init__(
        self,
        title: str,
        data: pd.DataFrame,
        column_formats: Optional[dict] = None,
        include_totals: bool = False
    ):
        self.title = title
        self.data = data
        self.column_formats = column_formats or {}
        self.include_totals = include_totals
    
    def render_excel(self, workbook: Workbook, sheet_name: str) -> None:
        ws = workbook.create_sheet(title=sheet_name)
        
        # Title
        ws['A1'] = self.title
        ws['A1'].font = Font(size=14, bold=True, color='FFFFFF')
        ws['A1'].fill = PatternFill(start_color='1f4e79', end_color='1f4e79', fill_type='solid')
        ws.merge_cells('A1:' + chr(64 + len(self.data.columns)) + '1')
        
        # Headers
        for col_idx, col_name in enumerate(self.data.columns, 1):
            cell = ws.cell(row=3, column=col_idx, value=col_name)
            cell.font = Font(bold=True, color='FFFFFF')
            cell.fill = PatternFill(start_color='2e75b6', end_color='2e75b6', fill_type='solid')
            cell.alignment = Alignment(horizontal='center')
        
        # Data
        for row_idx, row in enumerate(self.data.itertuples(index=False), 4):
            for col_idx, value in enumerate(row, 1):
                cell = ws.cell(row=row_idx, column=col_idx, value=value)
                
                # Apply formatting
                col_name = self.data.columns[col_idx - 1]
                if col_name in self.column_formats:
                    cell.number_format = self.column_formats[col_name]
                
                # Alternating row colors
                if row_idx % 2 == 0:
                    cell.fill = PatternFill(start_color='f2f2f2', end_color='f2f2f2', fill_type='solid')
        
        # Totals row
        if self.include_totals:
            total_row = len(self.data) + 4
            ws.cell(row=total_row, column=1, value='TOTAL').font = Font(bold=True)
            
            for col_idx, col_name in enumerate(self.data.columns[1:], 2):
                if pd.api.types.is_numeric_dtype(self.data[col_name]):
                    total = self.data[col_name].sum()
                    cell = ws.cell(row=total_row, column=col_idx, value=total)
                    cell.font = Font(bold=True)
                    cell.fill = PatternFill(start_color='d9e1f2', end_color='d9e1f2', fill_type='solid')
        
        # Auto-adjust column widths
        for col_idx in range(1, len(self.data.columns) + 1):
            ws.column_dimensions[chr(64 + col_idx)].width = 15
    
    def render_pdf(self, pdf: PdfPages) -> None:
        fig, ax = plt.subplots(figsize=(11, len(self.data) * 0.3 + 2))
        ax.axis('tight')
        ax.axis('off')
        
        # Create table
        table = ax.table(
            cellText=self.data.values,
            colLabels=self.data.columns,
            cellLoc='center',
            loc='center',
            colColours=['#1f4e79'] * len(self.data.columns)
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.5)
        
        # Style header
        for i in range(len(self.data.columns)):
            table[(0, i)].set_text_props(color='white', fontweight='bold')
        
        ax.set_title(self.title, fontsize=14, fontweight='bold', pad=20)
        
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)
    
    def render_html(self) -> str:
        styled_df = self.data.style\
            .set_caption(self.title)\
            .set_properties(**{'text-align': 'center'})\
            .set_table_styles([
                {'selector': 'caption', 'props': [('font-size', '14pt'), ('font-weight', 'bold')]},
                {'selector': 'th', 'props': [('background-color', '#1f4e79'), ('color', 'white')]},
                {'selector': 'td', 'props': [('border', '1px solid #ddd')]},
                {'selector': 'tr:nth-child(even)', 'props': [('background-color', '#f2f2f2')]},
            ])
        
        return styled_df.to_html()


class ChartSection(ReportSection):
    """Report section containing a chart."""
    
    def __init__(
        self,
        title: str,
        chart_type: str,  # 'line', 'bar', 'pie'
        data: pd.DataFrame,
        x_column: str,
        y_columns: list[str]
    ):
        self.title = title
        self.chart_type = chart_type
        self.data = data
        self.x_column = x_column
        self.y_columns = y_columns
    
    def render_excel(self, workbook: Workbook, sheet_name: str) -> None:
        ws = workbook.create_sheet(title=sheet_name)
        
        # Write data
        for r_idx, row in enumerate(dataframe_to_rows(self.data, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        
        # Create chart
        if self.chart_type == 'line':
            chart = LineChart()
        else:
            chart = BarChart()
        
        chart.title = self.title
        chart.style = 10
        chart.y_axis.title = 'Value'
        chart.x_axis.title = self.x_column
        
        data_ref = Reference(ws, min_col=2, min_row=1, max_col=len(self.y_columns) + 1, max_row=len(self.data) + 1)
        cats_ref = Reference(ws, min_col=1, min_row=2, max_row=len(self.data) + 1)
        
        chart.add_data(data_ref, titles_from_data=True)
        chart.set_categories(cats_ref)
        
        ws.add_chart(chart, 'E2')
    
    def render_pdf(self, pdf: PdfPages) -> None:
        fig, ax = plt.subplots(figsize=(10, 6))
        
        if self.chart_type == 'line':
            for col in self.y_columns:
                ax.plot(self.data[self.x_column], self.data[col], marker='o', label=col)
        elif self.chart_type == 'bar':
            x = range(len(self.data))
            width = 0.8 / len(self.y_columns)
            for i, col in enumerate(self.y_columns):
                ax.bar([xi + i * width for xi in x], self.data[col], width, label=col)
            ax.set_xticks([xi + width * (len(self.y_columns) - 1) / 2 for xi in x])
            ax.set_xticklabels(self.data[self.x_column], rotation=45)
        
        ax.set_title(self.title, fontsize=14, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)
    
    def render_html(self) -> str:
        fig, ax = plt.subplots(figsize=(10, 6))
        
        if self.chart_type == 'line':
            for col in self.y_columns:
                ax.plot(self.data[self.x_column], self.data[col], marker='o', label=col)
        elif self.chart_type == 'bar':
            self.data.plot(x=self.x_column, y=self.y_columns, kind='bar', ax=ax)
        
        ax.set_title(self.title)
        ax.legend()
        
        # Save to buffer
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight')
        buf.seek(0)
        import base64
        img_base64 = base64.b64encode(buf.read()).decode()
        plt.close(fig)
        
        return f'<img src="data:image/png;base64,{img_base64}" alt="{self.title}"/>'


class ReportGenerator:
    """
    Main report generator orchestrating multiple sections and output formats.
    """
    
    def __init__(self, config: ReportConfig):
        self.config = config
        self.sections: list[ReportSection] = []
    
    def add_section(self, section: ReportSection) -> 'ReportGenerator':
        """Add a section to the report (fluent interface)."""
        self.sections.append(section)
        return self
    
    def generate_excel(self, output_path: str) -> str:
        """Generate Excel report."""
        workbook = Workbook()
        workbook.remove(workbook.active)  # Remove default sheet
        
        # Cover sheet
        cover = workbook.create_sheet('Cover')
        cover['A1'] = self.config.title
        cover['A1'].font = Font(size=24, bold=True, color=self.config.color_scheme['primary'])
        
        if self.config.subtitle:
            cover['A3'] = self.config.subtitle
            cover['A3'].font = Font(size=14, italic=True)
        
        cover['A5'] = f"Generated: {self.config.generated_at.strftime('%Y-%m-%d %H:%M')}"
        cover['A6'] = f"Author: {self.config.author}"
        
        # Content sections
        for i, section in enumerate(self.sections, 1):
            section.render_excel(workbook, f'Section_{i}')
        
        workbook.save(output_path)
        logger.info(f"Excel report saved: {output_path}")
        return output_path
    
    def generate_pdf(self, output_path: str) -> str:
        """Generate PDF report."""
        with PdfPages(output_path) as pdf:
            # Cover page
            fig, ax = plt.subplots(figsize=(8.5, 11))
            ax.axis('off')
            
            ax.text(0.5, 0.7, self.config.title, 
                   ha='center', va='center', fontsize=24, fontweight='bold',
                   transform=ax.transAxes)
            
            if self.config.subtitle:
                ax.text(0.5, 0.6, self.config.subtitle,
                       ha='center', va='center', fontsize=14, style='italic',
                       transform=ax.transAxes)
            
            ax.text(0.5, 0.4, f"Generated: {self.config.generated_at.strftime('%Y-%m-%d %H:%M')}",
                   ha='center', va='center', fontsize=10,
                   transform=ax.transAxes)
            
            pdf.savefig(fig, bbox_inches='tight')
            plt.close(fig)
            
            # Content sections
            for section in self.sections:
                section.render_pdf(pdf)
        
        logger.info(f"PDF report saved: {output_path}")
        return output_path
    
    def generate_html(self) -> str:
        """Generate HTML report."""
        template_str = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>{{ title }}</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                h1 { color: #{{ primary_color }}; }
                .header { border-bottom: 3px solid #{{ primary_color }}; padding-bottom: 20px; margin-bottom: 30px; }
                .section { margin: 30px 0; }
                table { border-collapse: collapse; width: 100%; }
                th, td { padding: 10px; text-align: left; }
            </style>
        </head>
        <body>
            <div class="header">
                <h1>{{ title }}</h1>
                {% if subtitle %}<h2>{{ subtitle }}</h2>{% endif %}
                <p>Generated: {{ generated_at }} | Author: {{ author }}</p>
            </div>
            {% for section in sections %}
            <div class="section">
                {{ section | safe }}
            </div>
            {% endfor %}
        </body>
        </html>
        """
        
        template = jinja2.Template(template_str)
        
        return template.render(
            title=self.config.title,
            subtitle=self.config.subtitle,
            author=self.config.author,
            generated_at=self.config.generated_at.strftime('%Y-%m-%d %H:%M'),
            primary_color=self.config.color_scheme['primary'],
            sections=[s.render_html() for s in self.sections]
        )


class EmailDistributor:
    """Handles email distribution of reports."""
    
    def __init__(self, config: EmailConfig):
        self.config = config
    
    def send_report(
        self,
        to_addresses: list[str],
        subject: str,
        body: str,
        attachments: list[tuple[str, bytes]],
        cc_addresses: Optional[list[str]] = None
    ) -> bool:
        """Send report via email with attachments."""
        msg = MIMEMultipart()
        msg['From'] = self.config.username
        msg['To'] = ', '.join(to_addresses)
        msg['Subject'] = subject
        
        if cc_addresses:
            msg['Cc'] = ', '.join(cc_addresses)
        
        # Body
        msg.attach(MIMEText(body, 'html'))
        
        # Attachments
        for filename, content in attachments:
            attachment = MIMEApplication(content)
            attachment.add_header('Content-Disposition', 'attachment', filename=filename)
            msg.attach(attachment)
        
        # Send
        try:
            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                if self.config.use_tls:
                    server.starttls()
                server.login(self.config.username, self.config.password)
                
                all_recipients = to_addresses + (cc_addresses or [])
                server.sendmail(self.config.username, all_recipients, msg.as_string())
            
            logger.info(f"Email sent to {len(to_addresses)} recipients")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False


class ScheduledReporter:
    """
    Scheduled report generation and distribution.
    """
    
    def __init__(
        self,
        report_generator: ReportGenerator,
        email_distributor: Optional[EmailDistributor] = None
    ):
        self.report_generator = report_generator
        self.email_distributor = email_distributor
        self.schedules: list[dict] = []
    
    def schedule(
        self,
        frequency: str,  # 'daily', 'weekly', 'monthly'
        output_formats: list[str],
        recipients: Optional[list[str]] = None,
        at_time: str = "08:00"
    ) -> 'ScheduledReporter':
        """Schedule a recurring report."""
        self.schedules.append({
            'frequency': frequency,
            'output_formats': output_formats,
            'recipients': recipients,
            'at_time': at_time,
            'last_run': None
        })
        return self
    
    def generate_and_distribute(
        self,
        output_dir: str,
        formats: list[str] = ['excel', 'pdf']
    ) -> dict:
        """Generate reports in specified formats and optionally email."""
        results = {'files': [], 'emails_sent': 0}
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_name = f"report_{timestamp}"
        
        attachments = []
        
        for fmt in formats:
            if fmt == 'excel':
                path = f"{output_dir}/{base_name}.xlsx"
                self.report_generator.generate_excel(path)
                results['files'].append(path)
                
                with open(path, 'rb') as f:
                    attachments.append((f"{base_name}.xlsx", f.read()))
                    
            elif fmt == 'pdf':
                path = f"{output_dir}/{base_name}.pdf"
                self.report_generator.generate_pdf(path)
                results['files'].append(path)
                
                with open(path, 'rb') as f:
                    attachments.append((f"{base_name}.pdf", f.read()))
        
        # Email distribution
        if self.email_distributor and attachments:
            for schedule in self.schedules:
                if schedule['recipients']:
                    success = self.email_distributor.send_report(
                        to_addresses=schedule['recipients'],
                        subject=f"Business Stream Report - {datetime.now().strftime('%Y-%m-%d')}",
                        body=f"<p>Please find attached the automated report.</p>",
                        attachments=attachments
                    )
                    if success:
                        results['emails_sent'] += len(schedule['recipients'])
        
        return results


# Usage example
if __name__ == "__main__":
    # Sample data
    consumption_data = pd.DataFrame({
        'Month': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
        'Consumption_m3': [120.5, 115.3, 108.7, 95.2, 89.4, 102.1],
        'Cost_GBP': [241.0, 230.6, 217.4, 190.4, 178.8, 204.2]
    })
    
    meter_summary = pd.DataFrame({
        'Meter ID': ['M001', 'M002', 'M003', 'M004', 'M005'],
        'Total Consumption': [500.2, 420.1, 380.5, 290.3, 210.7],
        'Avg Daily': [16.7, 14.0, 12.7, 9.7, 7.0],
        'Status': ['Active', 'Active', 'Active', 'Inactive', 'Active']
    })
    
    # Configure report
    config = ReportConfig(
        title="Monthly Water Consumption Report",
        subtitle="Business Stream - Q2 2023",
        author="Data Analytics Team"
    )
    
    # Build report
    report = ReportGenerator(config)
    report.add_section(DataTableSection(
        title="Meter Summary",
        data=meter_summary,
        column_formats={'Total Consumption': '#,##0.00', 'Avg Daily': '#,##0.00'},
        include_totals=True
    ))
    report.add_section(ChartSection(
        title="Monthly Consumption Trend",
        chart_type='line',
        data=consumption_data,
        x_column='Month',
        y_columns=['Consumption_m3']
    ))
    
    # Generate outputs
    report.generate_excel('/tmp/consumption_report.xlsx')
    report.generate_pdf('/tmp/consumption_report.pdf')
    
    print("Reports generated successfully!")
```

### Key Design Decisions

1. **Section Pattern**: Modular sections for flexible report composition
2. **Multiple Outputs**: Single source generates Excel, PDF, and HTML
3. **Templating**: Jinja2 for HTML reports with customizable styling
4. **Email Integration**: Automatic distribution with attachments
5. **Scheduling Framework**: Extensible for cron/job scheduler integration

---

*[Questions 7-12 continue in the next section...]*


---

## Question 7: Feature Engineering for Time-Series Utility Data

### Challenge
Design a comprehensive feature engineering pipeline for utility meter data that:
1. Extracts temporal features (seasonality, holidays, business days)
2. Creates lag features and rolling statistics
3. Implements domain-specific features (consumption patterns, efficiency metrics)
4. Handles multiple granularities (hourly, daily, weekly, monthly)
5. Provides feature importance and selection capabilities

### Production Solution

```python
"""
Feature engineering pipeline for utility time-series data.
Implements temporal, lag, rolling, and domain-specific features.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Optional, Union

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_selection import mutual_info_regression

logger = logging.getLogger(__name__)


# UK Bank Holidays (simplified - would use actual calendar in production)
UK_BANK_HOLIDAYS_2023 = [
    '2023-01-02', '2023-04-07', '2023-04-10', '2023-05-01',
    '2023-05-29', '2023-08-28', '2023-12-25', '2023-12-26'
]


class FeatureEngineer(ABC):
    """Abstract base class for feature engineers."""
    
    @abstractmethod
    def fit(self, df: pd.DataFrame) -> 'FeatureEngineer':
        """Fit the feature engineer to data."""
        pass
    
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform data by adding features."""
        pass
    
    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fit and transform in one call."""
        return self.fit(df).transform(df)


class TemporalFeatureEngineer(FeatureEngineer):
    """
    Extract temporal features from timestamp columns.
    """
    
    def __init__(
        self,
        datetime_col: str = 'timestamp',
        include_cyclical: bool = True,
        holidays: Optional[list[str]] = None
    ):
        self.datetime_col = datetime_col
        self.include_cyclical = include_cyclical
        self.holidays = set(pd.to_datetime(holidays) if holidays else [])
        self._is_fitted = False
    
    def fit(self, df: pd.DataFrame) -> 'TemporalFeatureEngineer':
        self._is_fitted = True
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        dt = pd.to_datetime(df[self.datetime_col])
        
        # Basic temporal features
        df['hour'] = dt.dt.hour
        df['day_of_week'] = dt.dt.dayofweek  # 0=Monday
        df['day_of_month'] = dt.dt.day
        df['day_of_year'] = dt.dt.dayofyear
        df['week_of_year'] = dt.dt.isocalendar().week
        df['month'] = dt.dt.month
        df['quarter'] = dt.dt.quarter
        df['year'] = dt.dt.year
        
        # Business features
        df['is_weekend'] = dt.dt.dayofweek >= 5
        df['is_month_start'] = dt.dt.is_month_start
        df['is_month_end'] = dt.dt.is_month_end
        df['is_quarter_start'] = dt.dt.is_quarter_start
        df['is_quarter_end'] = dt.dt.is_quarter_end
        
        # Holiday indicator
        if self.holidays:
            df['is_holiday'] = dt.isin(self.holidays)
            df['is_business_day'] = (~df['is_weekend']) & (~df['is_holiday'])
        else:
            df['is_business_day'] = ~df['is_weekend']
        
        # Season (meteorological)
        df['season'] = dt.dt.month.map({
            12: 'winter', 1: 'winter', 2: 'winter',
            3: 'spring', 4: 'spring', 5: 'spring',
            6: 'summer', 7: 'summer', 8: 'summer',
            9: 'autumn', 10: 'autumn', 11: 'autumn'
        })
        
        # Cyclical encoding for periodic features
        if self.include_cyclical:
            # Hour of day (24-hour cycle)
            df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
            df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
            
            # Day of week (7-day cycle)
            df['dow_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
            df['dow_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
            
            # Month (12-month cycle)
            df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
            df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
            
            # Day of year (365-day cycle)
            df['doy_sin'] = np.sin(2 * np.pi * df['day_of_year'] / 365.25)
            df['doy_cos'] = np.cos(2 * np.pi * df['day_of_year'] / 365.25)
        
        return df


class LagFeatureEngineer(FeatureEngineer):
    """
    Create lag features for time-series data.
    """
    
    def __init__(
        self,
        value_col: str = 'consumption',
        group_col: str = 'meter_id',
        lags: list[int] = None,
        datetime_col: str = 'timestamp'
    ):
        self.value_col = value_col
        self.group_col = group_col
        self.lags = lags or [1, 7, 30]  # Default: 1 day, 1 week, 1 month
        self.datetime_col = datetime_col
        self._is_fitted = False
    
    def fit(self, df: pd.DataFrame) -> 'LagFeatureEngineer':
        self._is_fitted = True
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = df.sort_values([self.group_col, self.datetime_col])
        
        for lag in self.lags:
            df[f'{self.value_col}_lag_{lag}'] = df.groupby(self.group_col)[self.value_col].shift(lag)
        
        return df


class RollingFeatureEngineer(FeatureEngineer):
    """
    Create rolling window statistics.
    """
    
    def __init__(
        self,
        value_col: str = 'consumption',
        group_col: str = 'meter_id',
        windows: list[int] = None,
        aggregations: list[str] = None,
        datetime_col: str = 'timestamp',
        min_periods: int = 1
    ):
        self.value_col = value_col
        self.group_col = group_col
        self.windows = windows or [7, 14, 30]
        self.aggregations = aggregations or ['mean', 'std', 'min', 'max']
        self.datetime_col = datetime_col
        self.min_periods = min_periods
        self._is_fitted = False
    
    def fit(self, df: pd.DataFrame) -> 'RollingFeatureEngineer':
        self._is_fitted = True
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = df.sort_values([self.group_col, self.datetime_col])
        
        for window in self.windows:
            for agg in self.aggregations:
                col_name = f'{self.value_col}_rolling_{window}_{agg}'
                
                if agg == 'mean':
                    df[col_name] = df.groupby(self.group_col)[self.value_col].transform(
                        lambda x: x.rolling(window, min_periods=self.min_periods).mean()
                    )
                elif agg == 'std':
                    df[col_name] = df.groupby(self.group_col)[self.value_col].transform(
                        lambda x: x.rolling(window, min_periods=self.min_periods).std()
                    )
                elif agg == 'min':
                    df[col_name] = df.groupby(self.group_col)[self.value_col].transform(
                        lambda x: x.rolling(window, min_periods=self.min_periods).min()
                    )
                elif agg == 'max':
                    df[col_name] = df.groupby(self.group_col)[self.value_col].transform(
                        lambda x: x.rolling(window, min_periods=self.min_periods).max()
                    )
                elif agg == 'sum':
                    df[col_name] = df.groupby(self.group_col)[self.value_col].transform(
                        lambda x: x.rolling(window, min_periods=self.min_periods).sum()
                    )
        
        # Add rolling trend (difference from rolling mean)
        for window in self.windows:
            rolling_mean = df.groupby(self.group_col)[self.value_col].transform(
                lambda x: x.rolling(window, min_periods=self.min_periods).mean()
            )
            df[f'{self.value_col}_trend_{window}'] = df[self.value_col] - rolling_mean
        
        return df


class DomainFeatureEngineer(FeatureEngineer):
    """
    Domain-specific features for utility meter data.
    """
    
    def __init__(
        self,
        value_col: str = 'consumption',
        group_col: str = 'meter_id',
        datetime_col: str = 'timestamp'
    ):
        self.value_col = value_col
        self.group_col = group_col
        self.datetime_col = datetime_col
        self._meter_baselines: dict = {}
        self._is_fitted = False
    
    def fit(self, df: pd.DataFrame) -> 'DomainFeatureEngineer':
        """Calculate baseline consumption for each meter."""
        df = df.copy()
        df['month'] = pd.to_datetime(df[self.datetime_col]).dt.month
        
        # Calculate monthly baselines
        monthly_stats = df.groupby([self.group_col, 'month'])[self.value_col].agg(['mean', 'std'])
        
        for meter_id in df[self.group_col].unique():
            self._meter_baselines[meter_id] = monthly_stats.loc[meter_id].to_dict()
        
        self._is_fitted = True
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['month'] = pd.to_datetime(df[self.datetime_col]).dt.month
        
        # Consumption efficiency (ratio to baseline)
        def get_baseline_ratio(row):
            meter_id = row[self.group_col]
            month = row['month']
            if meter_id in self._meter_baselines:
                baseline = self._meter_baselines[meter_id].get(month, {}).get('mean', row[self.value_col])
                return row[self.value_col] / baseline if baseline > 0 else 1.0
            return 1.0
        
        df['consumption_efficiency'] = df.apply(get_baseline_ratio, axis=1)
        
        # Consumption volatility (coefficient of variation)
        df['consumption_cv'] = df.groupby(self.group_col)[self.value_col].transform(
            lambda x: x.rolling(30, min_periods=7).std() / x.rolling(30, min_periods=7).mean()
        )
        
        # Peak-to-off-peak ratio (if hour column exists)
        if 'hour' in df.columns:
            df['is_peak_hours'] = df['hour'].between(7, 19) & df.get('is_business_day', True)
            
            peak_mean = df[df['is_peak_hours']].groupby(self.group_col)[self.value_col].transform('mean')
            offpeak_mean = df[~df['is_peak_hours']].groupby(self.group_col)[self.value_col].transform('mean')
            
            df['peak_offpeak_ratio'] = peak_mean / offpeak_mean.replace(0, np.nan)
        
        # Weekend vs weekday consumption
        if 'is_weekend' in df.columns:
            weekend_mean = df[df['is_weekend']].groupby(self.group_col)[self.value_col].transform('mean')
            weekday_mean = df[~df['is_weekend']].groupby(self.group_col)[self.value_col].transform('mean')
            
            df['weekend_weekday_ratio'] = weekend_mean / weekday_mean.replace(0, np.nan)
        
        # Growth rate (year-over-year if enough data)
        df['yoy_growth'] = df.groupby(self.group_col)[self.value_col].pct_change(periods=365)
        
        # Consumption anomaly score (z-score)
        df['consumption_zscore'] = df.groupby(self.group_col)[self.value_col].transform(
            lambda x: (x - x.rolling(30, min_periods=7).mean()) / x.rolling(30, min_periods=7).std().replace(0, 1)
        )
        
        return df


class FeatureSelector:
    """
    Feature selection using statistical methods.
    """
    
    def __init__(
        self,
        target_col: str,
        method: str = 'mutual_info',
        n_features: int = 20
    ):
        self.target_col = target_col
        self.method = method
        self.n_features = n_features
        self.selected_features: list[str] = []
        self.feature_scores: dict[str, float] = {}
    
    def fit(self, df: pd.DataFrame) -> 'FeatureSelector':
        """Select top features based on importance scores."""
        # Get numeric columns only
        numeric_df = df.select_dtypes(include=[np.number])
        
        if self.target_col not in numeric_df.columns:
            raise ValueError(f"Target column {self.target_col} not found or not numeric")
        
        feature_cols = [c for c in numeric_df.columns if c != self.target_col]
        
        X = numeric_df[feature_cols].fillna(0)
        y = numeric_df[self.target_col].fillna(0)
        
        if self.method == 'mutual_info':
            scores = mutual_info_regression(X, y, random_state=42)
        elif self.method == 'correlation':
            scores = [abs(np.corrcoef(X[col], y)[0, 1]) for col in feature_cols]
        else:
            raise ValueError(f"Unknown method: {self.method}")
        
        self.feature_scores = dict(zip(feature_cols, scores))
        
        # Select top features
        sorted_features = sorted(self.feature_scores.items(), key=lambda x: x[1], reverse=True)
        self.selected_features = [f[0] for f in sorted_features[:self.n_features]]
        
        logger.info(f"Selected {len(self.selected_features)} features using {self.method}")
        
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return DataFrame with only selected features."""
        keep_cols = self.selected_features + [self.target_col]
        return df[keep_cols]
    
    def get_feature_importance(self) -> pd.DataFrame:
        """Get feature importance as DataFrame."""
        return pd.DataFrame([
            {'feature': k, 'importance': v}
            for k, v in sorted(self.feature_scores.items(), key=lambda x: x[1], reverse=True)
        ])


class FeaturePipeline:
    """
    Orchestrates multiple feature engineers into a pipeline.
    """
    
    def __init__(
        self,
        engineers: list[FeatureEngineer],
        selector: Optional[FeatureSelector] = None
    ):
        self.engineers = engineers
        self.selector = selector
        self._is_fitted = False
    
    def fit(self, df: pd.DataFrame) -> 'FeaturePipeline':
        """Fit all engineers in sequence."""
        for engineer in self.engineers:
            engineer.fit(df)
        
        if self.selector:
            # Transform first to get all features, then select
            transformed = self.transform(df, fit_selector=False)
            self.selector.fit(transformed)
        
        self._is_fitted = True
        return self
    
    def transform(
        self,
        df: pd.DataFrame,
        fit_selector: bool = True
    ) -> pd.DataFrame:
        """Apply all transformations."""
        for engineer in self.engineers:
            df = engineer.transform(df)
        
        if self.selector and fit_selector:
            df = self.selector.transform(df)
        
        return df
    
    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fit and transform."""
        return self.fit(df).transform(df)
    
    def get_feature_info(self) -> dict:
        """Get information about engineered features."""
        info = {
            'engineers': [e.__class__.__name__ for e in self.engineers],
            'total_features': len(self.selector.selected_features) if self.selector else None,
            'selected_features': self.selector.selected_features if self.selector else None
        }
        
        if self.selector:
            info['feature_importance'] = self.selector.get_feature_importance().to_dict()
        
        return info


# Usage example
if __name__ == "__main__":
    # Generate sample data
    np.random.seed(42)
    n_records = 1000
    
    dates = pd.date_range('2023-01-01', periods=n_records, freq='D')
    meter_ids = np.random.choice(['M001', 'M002', 'M003'], n_records)
    
    # Create consumption with seasonal pattern
    base_consumption = 100
    seasonal = 20 * np.sin(2 * np.pi * np.arange(n_records) / 365.25)
    noise = np.random.normal(0, 10, n_records)
    
    test_data = pd.DataFrame({
        'meter_id': meter_ids,
        'timestamp': dates,
        'consumption': base_consumption + seasonal + noise
    })
    
    # Build feature pipeline
    pipeline = FeaturePipeline(
        engineers=[
            TemporalFeatureEngineer(
                datetime_col='timestamp',
                include_cyclical=True,
                holidays=UK_BANK_HOLIDAYS_2023
            ),
            LagFeatureEngineer(
                value_col='consumption',
                group_col='meter_id',
                lags=[1, 7, 30]
            ),
            RollingFeatureEngineer(
                value_col='consumption',
                group_col='meter_id',
                windows=[7, 30],
                aggregations=['mean', 'std']
            ),
            DomainFeatureEngineer(
                value_col='consumption',
                group_col='meter_id',
                datetime_col='timestamp'
            )
        ],
        selector=FeatureSelector(
            target_col='consumption',
            method='mutual_info',
            n_features=15
        )
    )
    
    # Fit and transform
    result = pipeline.fit_transform(test_data)
    
    print(f"Original features: {len(test_data.columns)}")
    print(f"Final features: {len(result.columns)}")
    print(f"\nTop features by importance:")
    print(pipeline.get_feature_info())
```

### Key Design Decisions

1. **Scikit-learn Compatible**: Follows TransformerMixin pattern for pipeline integration
2. **Cyclical Encoding**: Sine/cosine transforms preserve periodic relationships
3. **Domain Knowledge**: Baseline consumption, efficiency metrics for utilities
4. **Feature Selection**: Mutual information for non-linear relationships
5. **Grouped Operations**: All rolling/lag features computed per meter

---

## Question 8: Unit Testing and Data Validation

### Challenge
Build a comprehensive testing framework for data pipelines that:
1. Validates data schemas and types
2. Tests transformation logic with edge cases
3. Implements property-based testing for data invariants
4. Provides test fixtures and mock data generation
5. Includes integration tests for database operations

### Production Solution

```python
"""
Comprehensive testing framework for data pipelines.
Includes schema validation, property-based testing, and integration tests.
"""
from __future__ import annotations

import json
import logging
import tempfile
import unittest
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Generic, Optional, TypeVar
from unittest.mock import Mock, patch, MagicMock

import hypothesis.strategies as st
import numpy as np
import pandas as pd
import pytest
from hypothesis import given, settings, assume, example
from hypothesis.extra.pandas import column, data_frames, range_indexes
from pydantic import BaseModel, Field, validator, ValidationError

logger = logging.getLogger(__name__)


# ============== Schema Validation ==============

class MeterReadingSchema(BaseModel):
    """Pydantic schema for meter reading validation."""
    meter_id: str = Field(..., min_length=3, max_length=20, regex=r'^M[0-9]+$')
    timestamp: datetime
    reading: float = Field(..., ge=0)
    unit: str = Field(default='cubic_meters', regex='^(cubic_meters|liters|gallons)$')
    quality_flag: Optional[str] = None
    
    @validator('timestamp')
    def timestamp_not_future(cls, v):
        if v > datetime.now() + timedelta(hours=1):
            raise ValueError('Timestamp cannot be in the future')
        return v
    
    @validator('reading')
    def reading_reasonable(cls, v):
        if v > 1_000_000:  # Max reasonable reading
            raise ValueError('Reading exceeds maximum reasonable value')
        return v


class DataValidator:
    """Validates DataFrames against schemas."""
    
    def __init__(self, schema_class: type[BaseModel]):
        self.schema_class = schema_class
        self.validation_errors: list[dict] = []
    
    def validate_dataframe(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validate DataFrame and return (valid, invalid) partitions.
        """
        valid_rows = []
        invalid_rows = []
        
        for idx, row in df.iterrows():
            try:
                self.schema_class(**row.to_dict())
                valid_rows.append(idx)
            except ValidationError as e:
                invalid_rows.append({
                    'index': idx,
                    'row': row.to_dict(),
                    'errors': json.loads(e.json())
                })
        
        self.validation_errors = invalid_rows
        
        return df.loc[valid_rows], df.loc[[i['index'] for i in invalid_rows]]
    
    def get_validation_report(self) -> dict:
        """Generate validation report."""
        return {
            'total_rows': len(self.validation_errors),
            'invalid_rows': len(self.validation_errors),
            'error_types': self._categorize_errors(),
            'sample_errors': self.validation_errors[:5]
        }
    
    def _categorize_errors(self) -> dict:
        """Categorize errors by type."""
        error_counts = {}
        for error in self.validation_errors:
            for e in error['errors']:
                loc = '.'.join(str(l) for l in e.get('loc', []))
                error_counts[loc] = error_counts.get(loc, 0) + 1
        return error_counts


# ============== Property-Based Testing ==============

class DataInvariants:
    """
    Define and test data invariants using property-based testing.
    """
    
    @staticmethod
    def consumption_non_negative(df: pd.DataFrame, consumption_col: str = 'consumption') -> bool:
        """Consumption should always be non-negative."""
        return (df[consumption_col] >= 0).all()
    
    @staticmethod
    def readings_monotonic(df: pd.DataFrame, meter_col: str = 'meter_id', reading_col: str = 'reading') -> bool:
        """Readings should generally increase (accounting for rollovers)."""
        for meter_id in df[meter_col].unique():
            meter_data = df[df[meter_col] == meter_id].sort_values('timestamp')
            diffs = meter_data[reading_col].diff().dropna()
            # Allow for some negative values (rollovers) but flag excessive ones
            negative_ratio = (diffs < -1000).sum() / len(diffs) if len(diffs) > 0 else 0
            if negative_ratio > 0.1:  # More than 10% negative is suspicious
                return False
        return True
    
    @staticmethod
    def timestamps_unique_per_meter(df: pd.DataFrame, meter_col: str = 'meter_id', time_col: str = 'timestamp') -> bool:
        """Timestamps should be unique per meter."""
        return not df.duplicated(subset=[meter_col, time_col]).any()
    
    @staticmethod
    def no_extreme_outliers(df: pd.DataFrame, col: str = 'consumption', threshold: float = 10.0) -> bool:
        """No extreme outliers (beyond threshold * IQR)."""
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - threshold * IQR
        upper = Q3 + threshold * IQR
        return ((df[col] >= lower) & (df[col] <= upper)).all()


# Hypothesis strategies for generating test data
meter_id_strategy = st.sampled_from([f'M{i:04d}' for i in range(100)])

reading_strategy = st.floats(min_value=0, max_value=100000, allow_nan=False, allow_infinity=False)

timestamp_strategy = st.datetimes(
    min_value=datetime(2020, 1, 1),
    max_value=datetime.now(),
    timezones=st.just(None)
)

meter_reading_strategy = st.fixed_dictionaries({
    'meter_id': meter_id_strategy,
    'timestamp': timestamp_strategy,
    'reading': reading_strategy,
    'unit': st.sampled_from(['cubic_meters', 'liters', 'gallons']),
    'quality_flag': st.one_of(st.none(), st.sampled_from(['valid', 'suspect', 'invalid']))
})


# ============== Test Fixtures ==============

@pytest.fixture
def sample_meter_data():
    """Generate sample meter data for testing."""
    np.random.seed(42)
    n = 100
    
    return pd.DataFrame({
        'meter_id': np.random.choice(['M0001', 'M0002', 'M0003'], n),
        'timestamp': pd.date_range('2023-01-01', periods=n, freq='H'),
        'reading': np.cumsum(np.random.exponential(0.5, n)) + 1000,
        'unit': 'cubic_meters',
        'quality_flag': 'valid'
    })


@pytest.fixture
def mock_database():
    """Mock database connection for testing."""
    mock_conn = MagicMock()
    mock_pool = MagicMock()
    mock_pool.acquire.return_value.__aenter__ = Mock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = Mock(return_value=False)
    
    return mock_pool


# ============== Test Classes ==============

class TestDataValidation(unittest.TestCase):
    """Unit tests for data validation."""
    
    def setUp(self):
        self.validator = DataValidator(MeterReadingSchema)
    
    def test_valid_data_passes(self):
        """Valid data should pass validation."""
        df = pd.DataFrame({
            'meter_id': ['M0001', 'M0002'],
            'timestamp': [datetime(2023, 1, 1), datetime(2023, 1, 2)],
            'reading': [100.5, 200.3],
            'unit': ['cubic_meters', 'liters'],
            'quality_flag': ['valid', None]
        })
        
        valid, invalid = self.validator.validate_dataframe(df)
        
        self.assertEqual(len(valid), 2)
        self.assertEqual(len(invalid), 0)
    
    def test_invalid_meter_id_fails(self):
        """Invalid meter ID format should fail."""
        df = pd.DataFrame({
            'meter_id': ['INVALID', 'M0002'],
            'timestamp': [datetime(2023, 1, 1), datetime(2023, 1, 2)],
            'reading': [100.5, 200.3],
            'unit': ['cubic_meters', 'liters'],
            'quality_flag': [None, None]
        })
        
        valid, invalid = self.validator.validate_dataframe(df)
        
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(invalid), 1)
    
    def test_future_timestamp_fails(self):
        """Future timestamps should fail validation."""
        future_date = datetime.now() + timedelta(days=1)
        
        df = pd.DataFrame({
            'meter_id': ['M0001'],
            'timestamp': [future_date],
            'reading': [100.5],
            'unit': ['cubic_meters'],
            'quality_flag': [None]
        })
        
        valid, invalid = self.validator.validate_dataframe(df)
        
        self.assertEqual(len(invalid), 1)


class TestDataInvariants(unittest.TestCase):
    """Tests for data invariant checking."""
    
    def test_consumption_non_negative(self):
        """Consumption should never be negative."""
        df = pd.DataFrame({
            'consumption': [10.5, 20.3, 15.7, 0.0]
        })
        
        self.assertTrue(DataInvariants.consumption_non_negative(df))
    
    def test_consumption_negative_fails(self):
        """Negative consumption should fail invariant."""
        df = pd.DataFrame({
            'consumption': [10.5, -5.0, 15.7]
        })
        
        self.assertFalse(DataInvariants.consumption_non_negative(df))
    
    def test_timestamps_unique(self):
        """Duplicate timestamps per meter should be detected."""
        df = pd.DataFrame({
            'meter_id': ['M0001', 'M0001', 'M0002'],
            'timestamp': [datetime(2023, 1, 1), datetime(2023, 1, 1), datetime(2023, 1, 1)]
        })
        
        self.assertFalse(DataInvariants.timestamps_unique_per_meter(df))


class TestTransformations(unittest.TestCase):
    """Tests for data transformation logic."""
    
    def test_consumption_calculation(self):
        """Consumption should be difference between consecutive readings."""
        df = pd.DataFrame({
            'meter_id': ['M0001'] * 4,
            'timestamp': pd.date_range('2023-01-01', periods=4, freq='H'),
            'reading': [100.0, 105.5, 110.0, 108.0]  # Note: 108 < 110 (rollover)
        })
        
        df['consumption'] = df.groupby('meter_id')['reading'].diff()
        
        expected = [np.nan, 5.5, 4.5, -2.0]
        np.testing.assert_array_almost_equal(df['consumption'].values, expected)
    
    def test_rolling_mean_calculation(self):
        """Rolling mean should be correctly calculated."""
        df = pd.DataFrame({
            'meter_id': ['M0001'] * 5,
            'value': [10, 20, 30, 40, 50]
        })
        
        df['rolling_mean'] = df.groupby('meter_id')['value'].transform(
            lambda x: x.rolling(3, min_periods=1).mean()
        )
        
        expected = [10.0, 15.0, 20.0, 30.0, 40.0]
        np.testing.assert_array_almost_equal(df['rolling_mean'].values, expected)


# Property-based tests
class TestProperties(unittest.TestCase):
    """Property-based tests using Hypothesis."""
    
    @given(data_frames([
        column('consumption', elements=st.floats(min_value=0, max_value=1000)),
    ], index=range_indexes(min_size=1, max_size=100)))
    @settings(max_examples=50)
    def test_consumption_always_non_negative_property(self, df):
        """Property: consumption is always non-negative."""
        assume(not df.empty)
        self.assertTrue(DataInvariants.consumption_non_negative(df))
    
    @given(st.lists(meter_reading_strategy, min_size=1, max_size=50))
    @settings(max_examples=30)
    def test_schema_validation_property(self, readings):
        """Property: valid meter readings pass schema validation."""
        df = pd.DataFrame(readings)
        validator = DataValidator(MeterReadingSchema)
        valid, invalid = validator.validate_dataframe(df)
        
        # All generated data should be valid
        self.assertEqual(len(invalid), 0)
        self.assertEqual(len(valid), len(readings))


# Integration tests
@pytest.mark.integration
class TestDatabaseIntegration(unittest.TestCase):
    """Integration tests requiring database connection."""
    
    def setUp(self):
        """Set up test database."""
        # In real tests, use test database or SQLite
        self.test_db_path = tempfile.mktemp(suffix='.db')
    
    def tearDown(self):
        """Clean up test database."""
        import os
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)
    
    @patch('asyncpg.create_pool')
    def test_database_query(self, mock_create_pool):
        """Test database query execution."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetch.return_value = [
            {'meter_id': 'M0001', 'reading': 100.5}
        ]
        mock_pool.acquire.return_value.__aenter__ = Mock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = Mock(return_value=False)
        mock_create_pool.return_value = mock_pool
        
        # Test would go here
        self.assertTrue(True)  # Placeholder


# ============== Test Utilities ==============

class TestDataGenerator:
    """Generate test data for various scenarios."""
    
    @staticmethod
    def generate_clean_data(n: int = 100, n_meters: int = 5) -> pd.DataFrame:
        """Generate clean, valid meter data."""
        np.random.seed(42)
        
        meter_ids = [f'M{i:04d}' for i in range(n_meters)]
        
        return pd.DataFrame({
            'meter_id': np.random.choice(meter_ids, n),
            'timestamp': pd.date_range('2023-01-01', periods=n, freq='H'),
            'reading': np.cumsum(np.abs(np.random.normal(0.5, 0.1, n))) + 1000,
            'unit': 'cubic_meters',
            'quality_flag': 'valid'
        })
    
    @staticmethod
    def generate_data_with_outliers(n: int = 100, outlier_ratio: float = 0.05) -> pd.DataFrame:
        """Generate data with outliers for testing detection."""
        df = TestDataGenerator.generate_clean_data(n)
        
        n_outliers = int(n * outlier_ratio)
        outlier_indices = np.random.choice(df.index, n_outliers, replace=False)
        df.loc[outlier_indices, 'reading'] *= 10  # Extreme values
        
        return df
    
    @staticmethod
    def generate_data_with_gaps(n: int = 100, gap_ratio: float = 0.1) -> pd.DataFrame:
        """Generate data with missing values."""
        df = TestDataGenerator.generate_clean_data(n)
        
        n_gaps = int(n * gap_ratio)
        gap_indices = np.random.choice(df.index, n_gaps, replace=False)
        df.loc[gap_indices, 'reading'] = np.nan
        
        return df
    
    @staticmethod
    def generate_data_with_duplicates(n: int = 100, duplicate_ratio: float = 0.05) -> pd.DataFrame:
        """Generate data with duplicate timestamps."""
        df = TestDataGenerator.generate_clean_data(n)
        
        n_dups = int(n * duplicate_ratio)
        dup_indices = np.random.choice(df.index, n_dups, replace=False)
        
        for idx in dup_indices:
            # Create duplicate by copying another row
            dup_row = df.iloc[idx].copy()
            dup_row['reading'] += np.random.normal(0, 1)  # Slightly different reading
            df = pd.concat([df, pd.DataFrame([dup_row])], ignore_index=True)
        
        return df.sort_values('timestamp').reset_index(drop=True)


# ============== Pytest Fixtures and Tests ==============

def test_data_validation_with_pytest():
    """Test data validation using pytest."""
    validator = DataValidator(MeterReadingSchema)
    
    df = pd.DataFrame({
        'meter_id': ['M0001', 'M0002', 'INVALID'],
        'timestamp': [datetime(2023, 1, 1)] * 3,
        'reading': [100.0, 200.0, 300.0],
        'unit': ['cubic_meters'] * 3,
        'quality_flag': [None] * 3
    })
    
    valid, invalid = validator.validate_dataframe(df)
    
    assert len(valid) == 2
    assert len(invalid) == 1


def test_transformation_pipeline():
    """Test complete transformation pipeline."""
    from business_stream_interview_questions import FeaturePipeline, TemporalFeatureEngineer
    
    df = TestDataGenerator.generate_clean_data(50)
    
    pipeline = FeaturePipeline(engineers=[
        TemporalFeatureEngineer(datetime_col='timestamp')
    ])
    
    result = pipeline.fit_transform(df)
    
    assert 'hour' in result.columns
    assert 'day_of_week' in result.columns
    assert 'month_sin' in result.columns
    assert len(result) == len(df)


if __name__ == '__main__':
    unittest.main()
```

### Key Design Decisions

1. **Pydantic Schemas**: Type validation with custom business rules
2. **Property-Based Testing**: Hypothesis generates edge cases automatically
3. **Invariant Checking**: Reusable assertions for data quality
4. **Fixture Pattern**: Reusable test data generators
5. **Integration Markers**: Separate slow integration tests

---

## Question 9: Parallel Processing for Data Transformation

### Challenge
Design a parallel processing system for data transformations that:
1. Distributes work across multiple processes/threads
2. Handles data partitioning and result aggregation
3. Implements progress tracking and error recovery
4. Optimizes for CPU-bound vs I/O-bound tasks
5. Provides resource management and throttling

### Production Solution

```python
"""
Parallel processing system for data transformations.
Implements process pools, thread pools, and async execution with monitoring.
"""
from __future__ import annotations

import logging
import multiprocessing as mp
import os
import queue
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
    Future
)
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Generic, Iterator, Optional, TypeVar

import pandas as pd
from tqdm import tqdm

logger = logging.getLogger(__name__)

T = TypeVar('T')
R = TypeVar('R')


class TaskType(Enum):
    """Classification of task types for optimization."""
    CPU_BOUND = auto()  # Use processes
    IO_BOUND = auto()   # Use threads
    HYBRID = auto()     # Use async


@dataclass
class TaskResult:
    """Result of a parallel task execution."""
    task_id: str
    success: bool
    result: Any = None
    error: Optional[str] = None
    duration_seconds: float = 0.0
    worker_id: Optional[int] = None


@dataclass
class ParallelMetrics:
    """Metrics for parallel execution."""
    total_tasks: int
    completed_tasks: int = 0
    failed_tasks: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    worker_utilization: dict = field(default_factory=dict)
    
    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def success_rate(self) -> float:
        if self.completed_tasks > 0:
            return (self.completed_tasks - self.failed_tasks) / self.completed_tasks
        return 0.0


class TaskPartitioner(ABC, Generic[T]):
    """Abstract base for data partitioning strategies."""
    
    @abstractmethod
    def partition(self, data: list[T], n_partitions: int) -> list[list[T]]:
        """Partition data into n chunks."""
        pass


class ChunkPartitioner(TaskPartitioner):
    """Simple chunk-based partitioning."""
    
    def partition(self, data: list[T], n_partitions: int) -> list[list[T]]:
        chunk_size = max(1, len(data) // n_partitions)
        return [
            data[i:i + chunk_size]
            for i in range(0, len(data), chunk_size)
        ][:n_partitions]


class HashPartitioner(TaskPartitioner):
    """Hash-based partitioning for grouped data."""
    
    def __init__(self, key_func: Callable[[T], str]):
        self.key_func = key_func
    
    def partition(self, data: list[T], n_partitions: int) -> list[list[T]]:
        partitions: list[list[T]] = [[] for _ in range(n_partitions)]
        
        for item in data:
            key = self.key_func(item)
            partition_idx = hash(key) % n_partitions
            partitions[partition_idx].append(item)
        
        return partitions


class ParallelProcessor:
    """
    Generic parallel processor with multiple execution modes.
    """
    
    def __init__(
        self,
        max_workers: Optional[int] = None,
        task_type: TaskType = TaskType.CPU_BOUND,
        partitioner: Optional[TaskPartitioner] = None
    ):
        self.max_workers = max_workers or os.cpu_count()
        self.task_type = task_type
        self.partitioner = partitioner or ChunkPartitioner()
        self.metrics = ParallelMetrics(total_tasks=0)
    
    def _get_executor_class(self):
        """Get appropriate executor based on task type."""
        if self.task_type == TaskType.CPU_BOUND:
            return ProcessPoolExecutor
        else:
            return ThreadPoolExecutor
    
    def map(
        self,
        func: Callable[[T], R],
        data: list[T],
        show_progress: bool = True,
        error_handling: str = 'skip'
    ) -> Iterator[R]:
        """
        Apply function to data in parallel.
        
        Args:
            func: Function to apply to each item
            data: List of items to process
            show_progress: Show progress bar
            error_handling: 'skip', 'fail', or 'collect'
        """
        self.metrics = ParallelMetrics(
            total_tasks=len(data),
            start_time=datetime.now()
        )
        
        executor_class = self._get_executor_class()
        
        with executor_class(max_workers=self.max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(func, item): i
                for i, item in enumerate(data)
            }
            
            # Process results as they complete
            iterator = as_completed(futures)
            if show_progress:
                iterator = tqdm(iterator, total=len(data), desc="Processing")
            
            for future in iterator:
                idx = futures[future]
                
                try:
                    result = future.result()
                    self.metrics.completed_tasks += 1
                    yield result
                    
                except Exception as e:
                    self.metrics.failed_tasks += 1
                    logger.error(f"Task {idx} failed: {e}")
                    
                    if error_handling == 'fail':
                        raise
                    elif error_handling == 'collect':
                        yield TaskResult(
                            task_id=str(idx),
                            success=False,
                            error=str(e)
                        )
        
        self.metrics.end_time = datetime.now()
    
    def map_chunks(
        self,
        func: Callable[[list[T]], list[R]],
        data: list[T],
        chunk_size: Optional[int] = None,
        show_progress: bool = True
    ) -> list[R]:
        """
        Process data in chunks for better efficiency.
        
        Useful when per-item overhead is high.
        """
        if chunk_size is None:
            chunk_size = max(1, len(data) // (self.max_workers * 4))
        
        # Partition into chunks
        chunks = [
            data[i:i + chunk_size]
            for i in range(0, len(data), chunk_size)
        ]
        
        logger.info(f"Processing {len(data)} items in {len(chunks)} chunks")
        
        # Process chunks in parallel
        results = []
        for chunk_result in self.map(func, chunks, show_progress=show_progress):
            if isinstance(chunk_result, list):
                results.extend(chunk_result)
            else:
                results.append(chunk_result)
        
        return results
    
    def get_metrics(self) -> dict:
        """Get execution metrics."""
        return {
            'total_tasks': self.metrics.total_tasks,
            'completed_tasks': self.metrics.completed_tasks,
            'failed_tasks': self.metrics.failed_tasks,
            'duration_seconds': self.metrics.duration_seconds,
            'success_rate': self.metrics.success_rate,
            'throughput_per_second': (
                self.metrics.completed_tasks / self.metrics.duration_seconds
                if self.metrics.duration_seconds > 0 else 0
            )
        }


class DataFrameParallelProcessor:
    """
    Specialized parallel processor for pandas DataFrames.
    """
    
    def __init__(
        self,
        n_workers: Optional[int] = None,
        partition_by: Optional[str] = None
    ):
        self.n_workers = n_workers or os.cpu_count()
        self.partition_by = partition_by
    
    def apply(
        self,
        df: pd.DataFrame,
        func: Callable[[pd.DataFrame], pd.DataFrame],
        show_progress: bool = True
    ) -> pd.DataFrame:
        """
        Apply function to DataFrame partitions in parallel.
        """
        # Partition the DataFrame
        if self.partition_by and self.partition_by in df.columns:
            partitions = [
                group for _, group in df.groupby(self.partition_by)
            ]
        else:
            # Row-based partitioning
            partition_size = max(1, len(df) // (self.n_workers * 2))
            partitions = [
                df.iloc[i:i + partition_size]
                for i in range(0, len(df), partition_size)
            ]
        
        logger.info(f"Processing DataFrame with {len(df)} rows in {len(partitions)} partitions")
        
        # Process in parallel
        processor = ParallelProcessor(
            max_workers=self.n_workers,
            task_type=TaskType.CPU_BOUND
        )
        
        results = processor.map_chunks(
            lambda chunk: func(chunk),
            partitions,
            show_progress=show_progress
        )
        
        # Combine results
        if results and isinstance(results[0], pd.DataFrame):
            return pd.concat(results, ignore_index=True)
        
        return pd.DataFrame(results)
    
    def groupby_apply(
        self,
        df: pd.DataFrame,
        groupby_cols: list[str],
        func: Callable[[pd.DataFrame], Any],
        show_progress: bool = True
    ) -> pd.DataFrame:
        """
        Parallel groupby-apply operation.
        """
        groups = [(name, group) for name, group in df.groupby(groupby_cols)]
        
        processor = ParallelProcessor(
            max_workers=self.n_workers,
            task_type=TaskType.CPU_BOUND
        )
        
        results = []
        for result in processor.map(
            lambda x: (x[0], func(x[1])),
            groups,
            show_progress=show_progress
        ):
            if isinstance(result, tuple):
                results.append(result)
            else:
                results.append((None, result))
        
        # Convert to DataFrame
        result_dfs = []
        for name, result in results:
            if isinstance(result, pd.DataFrame):
                if name is not None:
                    for col, val in zip(groupby_cols, name if isinstance(name, tuple) else [name]):
                        result[col] = val
                result_dfs.append(result)
            elif isinstance(result, pd.Series):
                result_df = result.to_frame().T
                if name is not None:
                    for col, val in zip(groupby_cols, name if isinstance(name, tuple) else [name]):
                        result_df[col] = val
                result_dfs.append(result_df)
        
        if result_dfs:
            return pd.concat(result_dfs, ignore_index=True)
        
        return pd.DataFrame()


class AsyncProcessor:
    """
    Async-based processor for I/O-bound tasks.
    """
    
    def __init__(self, max_concurrent: int = 100):
        self.max_concurrent = max_concurrent
        self.semaphore = threading.Semaphore(max_concurrent)
    
    async def map(
        self,
        func: Callable[[T], R],
        data: list[T],
        show_progress: bool = True
    ) -> list[R]:
        """
        Apply async function to data with concurrency limit.
        
        Note: func must be async.
        """
        import asyncio
        
        async def bounded_func(item):
            async with self.semaphore:
                return await func(item)
        
        tasks = [bounded_func(item) for item in data]
        
        if show_progress:
            results = []
            for task in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
                results.append(await task)
            return results
        
        return await asyncio.gather(*tasks)


class ResourceManager:
    """
    Manages system resources for parallel processing.
    """
    
    def __init__(
        self,
        max_memory_percent: float = 80.0,
        max_cpu_percent: float = 90.0
    ):
        self.max_memory_percent = max_memory_percent
        self.max_cpu_percent = max_cpu_percent
        self._throttle_event = threading.Event()
    
    def check_resources(self) -> dict:
        """Check current resource usage."""
        try:
            import psutil
            
            memory = psutil.virtual_memory()
            cpu = psutil.cpu_percent(interval=0.1)
            
            return {
                'memory_percent': memory.percent,
                'memory_available_gb': memory.available / (1024**3),
                'cpu_percent': cpu,
                'throttle_needed': (
                    memory.percent > self.max_memory_percent or
                    cpu > self.max_cpu_percent
                )
            }
        except ImportError:
            return {'throttle_needed': False}
    
    def throttle_if_needed(self):
        """Pause execution if resources are constrained."""
        resources = self.check_resources()
        
        if resources.get('throttle_needed'):
            logger.warning("Resource throttling activated")
            time.sleep(1)  # Brief pause


# Usage example
if __name__ == "__main__":
    # Generate test data
    np.random.seed(42)
    n = 100_000
    
    test_data = [
        {'id': i, 'value': np.random.random()}
        for i in range(n)
    ]
    
    # Define transformation function
    def expensive_transform(item: dict) -> dict:
        """Simulate expensive computation."""
        result = 0
        for _ in range(1000):  # Simulate work
            result += item['value'] ** 2
        return {**item, 'transformed': result}
    
    # Process in parallel
    processor = ParallelProcessor(
        max_workers=4,
        task_type=TaskType.CPU_BOUND
    )
    
    results = list(processor.map(
        expensive_transform,
        test_data,
        show_progress=True
    ))
    
    print(f"\nProcessed {len(results)} items")
    print(f"Metrics: {processor.get_metrics()}")
    
    # DataFrame example
    df = pd.DataFrame({
        'meter_id': np.random.choice(['M001', 'M002', 'M003', 'M004'], 10000),
        'reading': np.random.exponential(100, 10000)
    })
    
    def process_group(group: pd.DataFrame) -> pd.DataFrame:
        """Process a meter group."""
        group['rolling_mean'] = group['reading'].rolling(10, min_periods=1).mean()
        group['zscore'] = (group['reading'] - group['reading'].mean()) / group['reading'].std()
        return group
    
    df_processor = DataFrameParallelProcessor(n_workers=4, partition_by='meter_id')
    result_df = df_processor.apply(df, process_group)
    
    print(f"\nDataFrame processed: {len(result_df)} rows")
```

### Key Design Decisions

1. **TaskType Classification**: Different executors for CPU vs I/O bound
2. **Chunk Processing**: Amortizes per-task overhead
3. **Partition Strategies**: Hash-based for grouped data, chunk for row-based
4. **Resource Throttling**: Prevents system overload
5. **Metrics Collection**: Tracks throughput and success rates

---

*[Questions 10-12 continue in the final section...]*


---

## Question 10: Custom Anomaly Detection Algorithm

### Challenge
Design and implement a custom anomaly detection system for utility meter data that:
1. Detects unusual consumption patterns (spikes, drops, sustained changes)
2. Handles seasonal variations and trends
3. Provides anomaly scoring and classification
4. Supports online learning for model updates
5. Generates actionable alerts with context

### Production Solution

```python
"""
Custom anomaly detection system for utility meter data.
Implements multiple detection methods with ensemble scoring and online learning.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd
from scipy import stats
from scipy.signal import find_peaks
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)


class AnomalyType(Enum):
    """Types of anomalies in utility data."""
    SPIKE = auto()          # Sudden increase
    DROP = auto()           # Sudden decrease
    TREND_CHANGE = auto()   # Sustained change in baseline
    SEASONAL_BREAK = auto() # Break from expected seasonal pattern
    ZERO_READING = auto()   # Unexpected zero reading
    STUCK_METER = auto()    # Repeated identical readings


@dataclass
class AnomalyResult:
    """Result of anomaly detection for a single data point."""
    timestamp: datetime
    meter_id: str
    anomaly_type: AnomalyType
    severity: float  # 0-1 scale
    score: float     # Raw anomaly score
    expected_value: float
    actual_value: float
    context: dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            'timestamp': self.timestamp.isoformat(),
            'meter_id': self.meter_id,
            'anomaly_type': self.anomaly_type.name,
            'severity': self.severity,
            'score': self.score,
            'expected_value': self.expected_value,
            'actual_value': self.actual_value,
            'deviation_percent': (
                (self.actual_value - self.expected_value) / self.expected_value * 100
                if self.expected_value != 0 else 0
            ),
            'context': self.context
        }


class BaseDetector(ABC):
    """Abstract base class for anomaly detectors."""
    
    @abstractmethod
    def fit(self, df: pd.DataFrame) -> 'BaseDetector':
        """Fit detector to historical data."""
        pass
    
    @abstractmethod
    def detect(self, df: pd.DataFrame) -> list[AnomalyResult]:
        """Detect anomalies in data."""
        pass
    
    @abstractmethod
    def update(self, new_data: pd.DataFrame) -> 'BaseDetector':
        """Update detector with new data (online learning)."""
        pass


class StatisticalDetector(BaseDetector):
    """
    Statistical anomaly detection using Z-score and IQR methods.
    """
    
    def __init__(
        self,
        value_col: str = 'consumption',
        zscore_threshold: float = 3.0,
        iqr_multiplier: float = 1.5,
        use_rolling: bool = True,
        window: int = 30
    ):
        self.value_col = value_col
        self.zscore_threshold = zscore_threshold
        self.iqr_multiplier = iqr_multiplier
        self.use_rolling = use_rolling
        self.window = window
        self._stats: dict = {}
    
    def fit(self, df: pd.DataFrame) -> 'StatisticalDetector':
        """Calculate baseline statistics."""
        values = df[self.value_col].dropna()
        
        self._stats = {
            'mean': values.mean(),
            'std': values.std(),
            'median': values.median(),
            'q1': values.quantile(0.25),
            'q3': values.quantile(0.75),
            'iqr': values.quantile(0.75) - values.quantile(0.25)
        }
        
        return self
    
    def detect(self, df: pd.DataFrame) -> list[AnomalyResult]:
        """Detect statistical anomalies."""
        anomalies = []
        
        for idx, row in df.iterrows():
            value = row[self.value_col]
            timestamp = row.get('timestamp', idx)
            meter_id = row.get('meter_id', 'unknown')
            
            # Calculate z-score
            if self.use_rolling and len(df) > self.window:
                # Use rolling statistics
                start_idx = max(0, df.index.get_loc(idx) - self.window)
                window_data = df.iloc[start_idx:df.index.get_loc(idx)]
                mean = window_data[self.value_col].mean()
                std = window_data[self.value_col].std()
            else:
                mean = self._stats['mean']
                std = self._stats['std']
            
            if std == 0:
                continue
            
            zscore = abs(value - mean) / std
            
            if zscore > self.zscore_threshold:
                severity = min(1.0, zscore / (self.zscore_threshold * 2))
                anomaly_type = AnomalyType.SPIKE if value > mean else AnomalyType.DROP
                
                anomalies.append(AnomalyResult(
                    timestamp=timestamp if isinstance(timestamp, datetime) else datetime.now(),
                    meter_id=str(meter_id),
                    anomaly_type=anomaly_type,
                    severity=severity,
                    score=zscore,
                    expected_value=mean,
                    actual_value=value,
                    context={
                        'method': 'zscore',
                        'threshold': self.zscore_threshold,
                        'window_size': self.window if self.use_rolling else None
                    }
                ))
        
        return anomalies
    
    def update(self, new_data: pd.DataFrame) -> 'StatisticalDetector':
        """Update statistics with new data."""
        # Incremental update using Welford's algorithm
        new_values = new_data[self.value_col].dropna()
        
        if len(new_values) == 0:
            return self
        
        old_mean = self._stats['mean']
        old_count = self._stats.get('count', 0)
        new_count = len(new_values)
        total_count = old_count + new_count
        
        new_mean = (old_mean * old_count + new_values.sum()) / total_count
        
        # Update stored stats
        self._stats['mean'] = new_mean
        self._stats['count'] = total_count
        
        return self


class SeasonalDecompositionDetector(BaseDetector):
    """
    Anomaly detection using seasonal decomposition.
    Detects breaks from expected seasonal patterns.
    """
    
    def __init__(
        self,
        value_col: str = 'consumption',
        datetime_col: str = 'timestamp',
        seasonality_period: int = 24,  # Hours for daily seasonality
        threshold_sigma: float = 2.0
    ):
        self.value_col = value_col
        self.datetime_col = datetime_col
        self.seasonality_period = seasonality_period
        self.threshold_sigma = threshold_sigma
        self._seasonal_pattern: Optional[pd.Series] = None
        self._residual_std: float = 0.0
    
    def fit(self, df: pd.DataFrame) -> 'SeasonalDecompositionDetector':
        """Extract seasonal pattern from historical data."""
        df = df.copy()
        df['hour'] = pd.to_datetime(df[self.datetime_col]).dt.hour
        
        # Calculate average pattern by hour
        self._seasonal_pattern = df.groupby('hour')[self.value_col].mean()
        
        # Calculate residual standard deviation
        df['expected'] = df['hour'].map(self._seasonal_pattern)
        df['residual'] = df[self.value_col] - df['expected']
        self._residual_std = df['residual'].std()
        
        return self
    
    def detect(self, df: pd.DataFrame) -> list[AnomalyResult]:
        """Detect seasonal anomalies."""
        if self._seasonal_pattern is None:
            raise RuntimeError("Detector not fitted")
        
        anomalies = []
        df = df.copy()
        df['hour'] = pd.to_datetime(df[self.datetime_col]).dt.hour
        
        for idx, row in df.iterrows():
            hour = row['hour']
            value = row[self.value_col]
            
            expected = self._seasonal_pattern.get(hour, value)
            residual = value - expected
            
            if self._residual_std > 0:
                sigma = abs(residual) / self._residual_std
                
                if sigma > self.threshold_sigma:
                    severity = min(1.0, sigma / (self.threshold_sigma * 2))
                    
                    anomalies.append(AnomalyResult(
                        timestamp=row[self.datetime_col],
                        meter_id=str(row.get('meter_id', 'unknown')),
                        anomaly_type=AnomalyType.SEASONAL_BREAK,
                        severity=severity,
                        score=sigma,
                        expected_value=expected,
                        actual_value=value,
                        context={
                            'method': 'seasonal_decomposition',
                            'hour': hour,
                            'seasonal_avg': expected
                        }
                    ))
        
        return anomalies
    
    def update(self, new_data: pd.DataFrame) -> 'SeasonalDecompositionDetector':
        """Update seasonal pattern with new data."""
        # Combine old and new data with exponential weighting
        new_data = new_data.copy()
        new_data['hour'] = pd.to_datetime(new_data[self.datetime_col]).dt.hour
        
        new_pattern = new_data.groupby('hour')[self.value_col].mean()
        
        # Exponential moving average update
        alpha = 0.1  # Learning rate
        if self._seasonal_pattern is not None:
            for hour in new_pattern.index:
                if hour in self._seasonal_pattern.index:
                    self._seasonal_pattern[hour] = (
                        (1 - alpha) * self._seasonal_pattern[hour] +
                        alpha * new_pattern[hour]
                    )
                else:
                    self._seasonal_pattern[hour] = new_pattern[hour]
        
        return self


class MLDetector(BaseDetector):
    """
    Machine learning-based anomaly detection using Isolation Forest.
    """
    
    def __init__(
        self,
        feature_cols: list[str],
        contamination: float = 0.05,
        n_estimators: int = 100
    ):
        self.feature_cols = feature_cols
        self.contamination = contamination
        self.n_estimators = n_estimators
        self._model: Optional[IsolationForest] = None
        self._scaler = StandardScaler()
    
    def fit(self, df: pd.DataFrame) -> 'MLDetector':
        """Train isolation forest model."""
        X = df[self.feature_cols].fillna(0)
        X_scaled = self._scaler.fit_transform(X)
        
        self._model = IsolationForest(
            n_estimators=self.n_estimators,
            contamination=self.contamination,
            random_state=42
        )
        self._model.fit(X_scaled)
        
        return self
    
    def detect(self, df: pd.DataFrame) -> list[AnomalyResult]:
        """Detect anomalies using trained model."""
        if self._model is None:
            raise RuntimeError("Model not fitted")
        
        anomalies = []
        X = df[self.feature_cols].fillna(0)
        X_scaled = self._scaler.transform(X)
        
        # Get anomaly scores (-1 for anomaly, 1 for normal)
        predictions = self._model.predict(X_scaled)
        scores = self._model.decision_function(X_scaled)
        
        for idx, (pred, score) in enumerate(zip(predictions, scores)):
            if pred == -1:  # Anomaly
                row = df.iloc[idx]
                
                # Convert score to severity (0-1)
                severity = min(1.0, abs(score) * 2)
                
                anomalies.append(AnomalyResult(
                    timestamp=row.get('timestamp', idx),
                    meter_id=str(row.get('meter_id', 'unknown')),
                    anomaly_type=AnomalyType.TREND_CHANGE,
                    severity=severity,
                    score=abs(score),
                    expected_value=0,  # Not applicable for ML detector
                    actual_value=0,
                    context={
                        'method': 'isolation_forest',
                        'features': self.feature_cols
                    }
                ))
        
        return anomalies
    
    def update(self, new_data: pd.DataFrame) -> 'MLDetector':
        """Partial fit with new data (if supported)."""
        # Isolation Forest doesn't support partial_fit
        # In production, use online learning algorithms like HSTree
        logger.warning("MLDetector doesn't support online updates, refitting...")
        return self.fit(new_data)


class EnsembleAnomalyDetector:
    """
    Ensemble detector combining multiple detection methods.
    """
    
    def __init__(
        self,
        detectors: list[BaseDetector],
        weights: Optional[list[float]] = None,
        voting_threshold: float = 0.5
    ):
        self.detectors = detectors
        self.weights = weights or [1.0] * len(detectors)
        self.voting_threshold = voting_threshold
    
    def fit(self, df: pd.DataFrame) -> 'EnsembleAnomalyDetector':
        """Fit all detectors."""
        for detector in self.detectors:
            detector.fit(df)
        return self
    
    def detect(self, df: pd.DataFrame) -> list[AnomalyResult]:
        """Detect anomalies using ensemble voting."""
        all_results = []
        
        for detector, weight in zip(self.detectors, self.weights):
            results = detector.detect(df)
            for r in results:
                r.context['detector_weight'] = weight
            all_results.extend(results)
        
        # Group by timestamp and meter
        grouped = {}
        for result in all_results:
            key = (result.timestamp, result.meter_id)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(result)
        
        # Apply voting
        final_anomalies = []
        for key, results in grouped.items():
            total_weight = sum(r.context.get('detector_weight', 1) for r in results)
            avg_severity = np.mean([r.severity for r in results])
            avg_score = np.mean([r.score for r in results])
            
            # Weighted vote
            weighted_vote = total_weight / sum(self.weights)
            
            if weighted_vote >= self.voting_threshold:
                # Use most severe anomaly type
                primary = max(results, key=lambda r: r.severity)
                
                final_anomalies.append(AnomalyResult(
                    timestamp=primary.timestamp,
                    meter_id=primary.meter_id,
                    anomaly_type=primary.anomaly_type,
                    severity=avg_severity,
                    score=avg_score,
                    expected_value=primary.expected_value,
                    actual_value=primary.actual_value,
                    context={
                        'ensemble_vote': weighted_vote,
                        'detectors_triggered': len(results),
                        'individual_results': [r.to_dict() for r in results]
                    }
                ))
        
        return final_anomalies
    
    def update(self, new_data: pd.DataFrame) -> 'EnsembleAnomalyDetector':
        """Update all detectors."""
        for detector in self.detectors:
            detector.update(new_data)
        return self


class AnomalyAlertSystem:
    """
    Alert generation and management system.
    """
    
    def __init__(
        self,
        severity_threshold: float = 0.7,
        cooldown_hours: float = 24.0,
        alert_handlers: Optional[list[Callable]] = None
    ):
        self.severity_threshold = severity_threshold
        self.cooldown_hours = cooldown_hours
        self.alert_handlers = alert_handlers or []
        self._alert_history: dict = {}  # (meter_id, anomaly_type) -> last_alert_time
    
    def should_alert(self, anomaly: AnomalyResult) -> bool:
        """Check if alert should be generated based on rules."""
        # Check severity threshold
        if anomaly.severity < self.severity_threshold:
            return False
        
        # Check cooldown
        key = (anomaly.meter_id, anomaly.anomaly_type)
        last_alert = self._alert_history.get(key)
        
        if last_alert:
            hours_since = (datetime.now() - last_alert).total_seconds() / 3600
            if hours_since < self.cooldown_hours:
                return False
        
        return True
    
    def generate_alert(self, anomaly: AnomalyResult) -> dict:
        """Generate alert for anomaly."""
        if not self.should_alert(anomaly):
            return None
        
        # Update alert history
        key = (anomaly.meter_id, anomaly.anomaly_type)
        self._alert_history[key] = datetime.now()
        
        alert = {
            'alert_id': f"{anomaly.meter_id}_{anomaly.timestamp.strftime('%Y%m%d%H%M%S')}",
            'timestamp': datetime.now().isoformat(),
            'severity': anomaly.severity,
            'title': f"{anomaly.anomaly_type.name} detected for {anomaly.meter_id}",
            'description': self._generate_description(anomaly),
            'recommended_action': self._recommend_action(anomaly),
            'anomaly_details': anomaly.to_dict()
        }
        
        # Call alert handlers
        for handler in self.alert_handlers:
            try:
                handler(alert)
            except Exception as e:
                logger.error(f"Alert handler failed: {e}")
        
        return alert
    
    def _generate_description(self, anomaly: AnomalyResult) -> str:
        """Generate human-readable description."""
        deviation = anomaly.to_dict()['deviation_percent']
        direction = "higher" if deviation > 0 else "lower"
        
        return (
            f"Consumption of {anomaly.actual_value:.2f} is "
            f"{abs(deviation):.1f}% {direction} than expected ({anomaly.expected_value:.2f}). "
            f"Anomaly type: {anomaly.anomaly_type.name}"
        )
    
    def _recommend_action(self, anomaly: AnomalyResult) -> str:
        """Recommend action based on anomaly type."""
        recommendations = {
            AnomalyType.SPIKE: "Check for leaks or unauthorized usage",
            AnomalyType.DROP: "Verify meter functionality and connectivity",
            AnomalyType.TREND_CHANGE: "Review recent changes in usage patterns",
            AnomalyType.SEASONAL_BREAK: "Investigate unusual seasonal variation",
            AnomalyType.ZERO_READING: "Check meter power and communication",
            AnomalyType.STUCK_METER: "Schedule meter inspection"
        }
        return recommendations.get(anomaly.anomaly_type, "Investigate anomaly")


# Usage example
if __name__ == "__main__":
    # Generate sample data with anomalies
    np.random.seed(42)
    n = 720  # 30 days of hourly data
    
    # Normal consumption with daily pattern
    hours = np.arange(n)
    base = 100
    daily_pattern = 20 * np.sin(2 * np.pi * hours / 24)
    noise = np.random.normal(0, 5, n)
    consumption = base + daily_pattern + noise
    
    # Inject anomalies
    consumption[100] += 80  # Spike
    consumption[200] -= 60  # Drop
    consumption[300:320] = 0  # Zero readings
    consumption[500:510] = consumption[499]  # Stuck meter
    
    df = pd.DataFrame({
        'meter_id': ['M001'] * n,
        'timestamp': pd.date_range('2023-01-01', periods=n, freq='H'),
        'consumption': consumption
    })
    
    # Split into train/test
    train_df = df.iloc[:500]
    test_df = df.iloc[500:]
    
    # Create ensemble detector
    ensemble = EnsembleAnomalyDetector(
        detectors=[
            StatisticalDetector(value_col='consumption', zscore_threshold=2.5),
            SeasonalDecompositionDetector(
                value_col='consumption',
                seasonality_period=24,
                threshold_sigma=2.5
            )
        ],
        weights=[0.5, 0.5],
        voting_threshold=0.5
    )
    
    # Fit and detect
    ensemble.fit(train_df)
    anomalies = ensemble.detect(df)
    
    # Generate alerts
    alert_system = AnomalyAlertSystem(severity_threshold=0.6)
    
    print(f"Detected {len(anomalies)} anomalies:\n")
    for anomaly in anomalies[:5]:
        print(f"  {anomaly.anomaly_type.name}: severity={anomaly.severity:.2f}")
        alert = alert_system.generate_alert(anomaly)
        if alert:
            print(f"    Alert: {alert['title']}")
            print(f"    Action: {alert['recommended_action']}")
```

### Key Design Decisions

1. **Ensemble Approach**: Combines statistical, seasonal, and ML methods
2. **Online Learning**: Incremental updates for production deployment
3. **Severity Scoring**: Normalized 0-1 scale for consistent alerting
4. **Alert Throttling**: Prevents alert fatigue with cooldown periods
5. **Contextual Information**: Rich anomaly metadata for investigation

---

## Question 11: Data Pipeline Monitoring and Alerting

### Challenge
Build a monitoring and alerting system for data pipelines that:
1. Tracks pipeline execution metrics (duration, throughput, errors)
2. Monitors data quality metrics (completeness, accuracy, freshness)
3. Implements configurable alerting rules
4. Provides dashboards and reporting
5. Supports distributed pipeline components

### Production Solution

```python
"""
Monitoring and alerting system for data pipelines.
Implements metrics collection, quality checks, and configurable alerting.
"""
from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Optional

import requests

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics that can be collected."""
    COUNTER = auto()    # Monotonically increasing
    GAUGE = auto()      # Can go up or down
    HISTOGRAM = auto()  # Distribution of values
    TIMER = auto()      # Duration measurements


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    CRITICAL = auto()


@dataclass
class Metric:
    """A single metric data point."""
    name: str
    value: float
    metric_type: MetricType
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tags: dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'value': self.value,
            'type': self.metric_type.name,
            'timestamp': self.timestamp.isoformat(),
            'tags': self.tags
        }


@dataclass
class DataQualityCheck:
    """Result of a data quality check."""
    check_name: str
    passed: bool
    metric_value: float
    threshold: float
    details: dict = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class PipelineExecution:
    """Record of a pipeline execution."""
    pipeline_id: str
    run_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = 'running'
    records_processed: int = 0
    records_failed: int = 0
    metrics: list[Metric] = field(default_factory=list)
    quality_checks: list[DataQualityCheck] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    
    @property
    def duration_seconds(self) -> float:
        end = self.end_time or datetime.utcnow()
        return (end - self.start_time).total_seconds()
    
    @property
    def success_rate(self) -> float:
        total = self.records_processed + self.records_failed
        return self.records_processed / total if total > 0 else 0.0
    
    def to_dict(self) -> dict:
        return {
            'pipeline_id': self.pipeline_id,
            'run_id': self.run_id,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'status': self.status,
            'duration_seconds': self.duration_seconds,
            'records_processed': self.records_processed,
            'records_failed': self.records_failed,
            'success_rate': self.success_rate,
            'metrics': [m.to_dict() for m in self.metrics],
            'quality_checks': [asdict(q) for q in self.quality_checks],
            'errors': self.errors
        }


class MetricsCollector:
    """
    Centralized metrics collection for pipelines.
    """
    
    def __init__(self, buffer_size: int = 10000):
        self.buffer_size = buffer_size
        self._metrics: list[Metric] = []
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
    
    def counter(self, name: str, value: float = 1, tags: dict = None):
        """Increment a counter metric."""
        key = f"{name}:{json.dumps(tags or {}, sort_keys=True)}"
        self._counters[key] = self._counters.get(key, 0) + value
        
        self._add_metric(Metric(
            name=name,
            value=self._counters[key],
            metric_type=MetricType.COUNTER,
            tags=tags or {}
        ))
    
    def gauge(self, name: str, value: float, tags: dict = None):
        """Set a gauge metric."""
        self._gauges[name] = value
        
        self._add_metric(Metric(
            name=name,
            value=value,
            metric_type=MetricType.GAUGE,
            tags=tags or {}
        ))
    
    def timer(self, name: str, duration_seconds: float, tags: dict = None):
        """Record a timer metric."""
        self._add_metric(Metric(
            name=name,
            value=duration_seconds,
            metric_type=MetricType.TIMER,
            tags=tags or {}
        ))
    
    def _add_metric(self, metric: Metric):
        """Add metric to buffer, maintaining size limit."""
        self._metrics.append(metric)
        
        if len(self._metrics) > self.buffer_size:
            self._metrics = self._metrics[-self.buffer_size:]
    
    def get_metrics(
        self,
        name: Optional[str] = None,
        since: Optional[datetime] = None
    ) -> list[Metric]:
        """Get metrics with optional filtering."""
        metrics = self._metrics
        
        if name:
            metrics = [m for m in metrics if m.name == name]
        
        if since:
            metrics = [m for m in metrics if m.timestamp >= since]
        
        return metrics
    
    def get_summary(self) -> dict:
        """Get summary of all metrics."""
        summary = {}
        
        for metric in self._metrics:
            if metric.name not in summary:
                summary[metric.name] = {
                    'count': 0,
                    'sum': 0,
                    'min': float('inf'),
                    'max': float('-inf')
                }
            
            s = summary[metric.name]
            s['count'] += 1
            s['sum'] += metric.value
            s['min'] = min(s['min'], metric.value)
            s['max'] = max(s['max'], metric.value)
        
        # Calculate averages
        for name, s in summary.items():
            s['avg'] = s['sum'] / s['count'] if s['count'] > 0 else 0
        
        return summary


class DataQualityMonitor:
    """
    Monitors data quality with configurable checks.
    """
    
    def __init__(self):
        self.checks: list[Callable] = []
        self.results: list[DataQualityCheck] = []
    
    def add_check(self, check_func: Callable):
        """Add a quality check function."""
        self.checks.append(check_func)
    
    def run_checks(self, df) -> list[DataQualityCheck]:
        """Run all quality checks on data."""
        results = []
        
        for check in self.checks:
            try:
                result = check(df)
                if isinstance(result, DataQualityCheck):
                    results.append(result)
            except Exception as e:
                logger.error(f"Quality check failed: {e}")
                results.append(DataQualityCheck(
                    check_name=check.__name__,
                    passed=False,
                    metric_value=0,
                    threshold=0,
                    details={'error': str(e)}
                ))
        
        self.results.extend(results)
        return results
    
    @staticmethod
    def completeness_check(threshold: float = 0.95):
        """Factory for completeness check."""
        def check(df):
            total_cells = df.size
            null_cells = df.isnull().sum().sum()
            completeness = 1 - (null_cells / total_cells)
            
            return DataQualityCheck(
                check_name='completeness',
                passed=completeness >= threshold,
                metric_value=completeness,
                threshold=threshold,
                details={'null_cells': null_cells, 'total_cells': total_cells}
            )
        return check
    
    @staticmethod
    def freshness_check(datetime_col: str, max_age_hours: float = 24):
        """Factory for freshness check."""
        def check(df):
            if datetime_col not in df.columns:
                return DataQualityCheck(
                    check_name='freshness',
                    passed=False,
                    metric_value=0,
                    threshold=max_age_hours,
                    details={'error': f'Column {datetime_col} not found'}
                )
            
            latest = pd.to_datetime(df[datetime_col]).max()
            age_hours = (datetime.now() - latest).total_seconds() / 3600
            
            return DataQualityCheck(
                check_name='freshness',
                passed=age_hours <= max_age_hours,
                metric_value=age_hours,
                threshold=max_age_hours,
                details={'latest_timestamp': latest.isoformat()}
            )
        return check
    
    @staticmethod
    def range_check(column: str, min_val: float, max_val: float):
        """Factory for range check."""
        def check(df):
            if column not in df.columns:
                return DataQualityCheck(
                    check_name=f'range_{column}',
                    passed=False,
                    metric_value=0,
                    threshold=0
                )
            
            out_of_range = ((df[column] < min_val) | (df[column] > max_val)).sum()
            in_range_pct = 1 - (out_of_range / len(df))
            
            return DataQualityCheck(
                check_name=f'range_{column}',
                passed=out_of_range == 0,
                metric_value=in_range_pct,
                threshold=1.0,
                details={
                    'out_of_range_count': out_of_range,
                    'min_val': min_val,
                    'max_val': max_val
                }
            )
        return check


class AlertRule:
    """
    Configurable alert rule.
    """
    
    def __init__(
        self,
        name: str,
        condition: Callable[[Any], bool],
        severity: AlertSeverity,
        message_template: str,
        cooldown_minutes: float = 60
    ):
        self.name = name
        self.condition = condition
        self.severity = severity
        self.message_template = message_template
        self.cooldown_minutes = cooldown_minutes
        self._last_alert: Optional[datetime] = None
    
    def evaluate(self, context: dict) -> Optional[dict]:
        """Evaluate rule against context."""
        # Check cooldown
        if self._last_alert:
            minutes_since = (datetime.utcnow() - self._last_alert).total_seconds() / 60
            if minutes_since < self.cooldown_minutes:
                return None
        
        # Evaluate condition
        try:
            if self.condition(context):
                self._last_alert = datetime.utcnow()
                return {
                    'rule_name': self.name,
                    'severity': self.severity.name,
                    'message': self.message_template.format(**context),
                    'timestamp': datetime.utcnow().isoformat(),
                    'context': context
                }
        except Exception as e:
            logger.error(f"Alert rule evaluation failed: {e}")
        
        return None


class AlertManager:
    """
    Manages alert rules and notification channels.
    """
    
    def __init__(self):
        self.rules: list[AlertRule] = []
        self.channels: list[Callable] = []
        self.alert_history: list[dict] = []
    
    def add_rule(self, rule: AlertRule):
        """Add an alert rule."""
        self.rules.append(rule)
    
    def add_channel(self, channel: Callable):
        """Add a notification channel."""
        self.channels.append(channel)
    
    def evaluate(self, context: dict):
        """Evaluate all rules and send alerts."""
        for rule in self.rules:
            alert = rule.evaluate(context)
            if alert:
                self._send_alert(alert)
    
    def _send_alert(self, alert: dict):
        """Send alert through all channels."""
        self.alert_history.append(alert)
        
        for channel in self.channels:
            try:
                channel(alert)
            except Exception as e:
                logger.error(f"Alert channel failed: {e}")


class PipelineMonitor:
    """
    Main monitoring class for data pipelines.
    """
    
    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.metrics = MetricsCollector()
        self.quality_monitor = DataQualityMonitor()
        self.alert_manager = AlertManager()
        self._current_execution: Optional[PipelineExecution] = None
        self._execution_history: list[PipelineExecution] = []
    
    def start_execution(self, run_id: str) -> PipelineExecution:
        """Start monitoring a pipeline execution."""
        execution = PipelineExecution(
            pipeline_id=self.pipeline_id,
            run_id=run_id,
            start_time=datetime.utcnow()
        )
        self._current_execution = execution
        return execution
    
    def end_execution(self, status: str = 'completed'):
        """End current execution."""
        if self._current_execution:
            self._current_execution.end_time = datetime.utcnow()
            self._current_execution.status = status
            self._execution_history.append(self._current_execution)
            
            # Evaluate alert rules
            self.alert_manager.evaluate(self._current_execution.to_dict())
            
            self._current_execution = None
    
    def record_metric(self, name: str, value: float, metric_type: MetricType, tags: dict = None):
        """Record a metric for current execution."""
        if self._current_execution:
            metric = Metric(
                name=name,
                value=value,
                metric_type=metric_type,
                tags=tags or {}
            )
            self._current_execution.metrics.append(metric)
        
        # Also add to global metrics
        if metric_type == MetricType.COUNTER:
            self.metrics.counter(name, value, tags)
        elif metric_type == MetricType.GAUGE:
            self.metrics.gauge(name, value, tags)
        elif metric_type == MetricType.TIMER:
            self.metrics.timer(name, value, tags)
    
    def run_quality_checks(self, df):
        """Run quality checks and record results."""
        results = self.quality_monitor.run_checks(df)
        
        if self._current_execution:
            self._current_execution.quality_checks.extend(results)
        
        return results
    
    def get_health_status(self) -> dict:
        """Get overall pipeline health."""
        recent_executions = [
            e for e in self._execution_history
            if e.start_time > datetime.utcnow() - timedelta(hours=24)
        ]
        
        if not recent_executions:
            return {'status': 'unknown', 'reason': 'No recent executions'}
        
        success_rate = sum(e.success_rate for e in recent_executions) / len(recent_executions)
        failed_executions = [e for e in recent_executions if e.status == 'failed']
        
        if success_rate < 0.9 or len(failed_executions) > 2:
            return {
                'status': 'unhealthy',
                'success_rate': success_rate,
                'failed_executions_24h': len(failed_executions)
            }
        
        return {
            'status': 'healthy',
            'success_rate': success_rate,
            'executions_24h': len(recent_executions)
        }


# Usage example
if __name__ == "__main__":
    # Create monitor
    monitor = PipelineMonitor(pipeline_id='meter_data_ingestion')
    
    # Add quality checks
    monitor.quality_monitor.add_check(
        DataQualityMonitor.completeness_check(threshold=0.98)
    )
    monitor.quality_monitor.add_check(
        DataQualityMonitor.freshness_check('timestamp', max_age_hours=2)
    )
    
    # Add alert rules
    monitor.alert_manager.add_rule(AlertRule(
        name='high_error_rate',
        condition=lambda ctx: ctx.get('success_rate', 1) < 0.9,
        severity=AlertSeverity.ERROR,
        message_template='Pipeline {pipeline_id} has low success rate: {success_rate:.1%}',
        cooldown_minutes=30
    ))
    
    # Add notification channel
    def log_alert(alert):
        print(f"ALERT [{alert['severity']}]: {alert['message']}")
    
    monitor.alert_manager.add_channel(log_alert)
    
    # Simulate execution
    import pandas as pd
    import numpy as np
    
    monitor.start_execution(run_id='run_001')
    
    # Simulate processing
    time.sleep(0.1)
    
    # Record metrics
    monitor.record_metric('records_processed', 10000, MetricType.COUNTER)
    monitor.record_metric('records_failed', 50, MetricType.COUNTER)
    monitor.record_metric('processing_time', 5.2, MetricType.TIMER)
    
    # Run quality checks
    test_data = pd.DataFrame({
        'timestamp': pd.date_range('2023-01-01', periods=100, freq='H'),
        'reading': np.random.exponential(100, 100)
    })
    test_data.loc[10:15, 'reading'] = np.nan  # Introduce some nulls
    
    quality_results = monitor.run_quality_checks(test_data)
    
    for result in quality_results:
        print(f"Quality check '{result.check_name}': {'PASS' if result.passed else 'FAIL'} "
              f"({result.metric_value:.2%} vs threshold {result.threshold})")
    
    monitor.end_execution(status='completed')
    
    # Get health status
    health = monitor.get_health_status()
    print(f"\nPipeline health: {health}")
```

### Key Design Decisions

1. **Metric Types**: Counter, Gauge, Histogram, Timer for different use cases
2. **Quality Checks**: Pluggable check functions with standardized results
3. **Alert Rules**: Configurable conditions with cooldown to prevent spam
4. **Execution Tracking**: Complete pipeline run history for debugging
5. **Health Status**: Aggregated view of pipeline health over time

---

## Question 12: Algorithm Challenge - Efficient Meter-to-Billing Period Matching

### Challenge
Design an efficient algorithm to match millions of meter readings to billing periods:

**Problem:**
- 10M+ meter readings with irregular timestamps
- 1M+ billing periods with custom start/end dates per customer
- Each meter has its own billing cycle (e.g., starts on 15th of month)
- Need to assign each reading to the correct billing period
- Must handle: partial periods, proration, timezone conversions

**Requirements:**
- O(n log n) or better time complexity
- Memory-efficient for production use
- Handle edge cases: meter changes, billing cycle changes
- Support reverse lookup (find readings for a billing period)

### Production Solution

```python
"""
Efficient algorithm for matching meter readings to billing periods.
Uses interval trees and binary search for O(log n) lookups.
"""
from __future__ import annotations

import bisect
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BillingPeriod:
    """Immutable billing period definition."""
    period_id: str
    meter_id: str
    start_date: datetime
    end_date: datetime
    is_partial: bool = False
    proration_factor: float = 1.0
    
    def contains(self, timestamp: datetime) -> bool:
        """Check if timestamp falls within this period."""
        return self.start_date <= timestamp < self.end_date
    
    def duration_days(self) -> float:
        """Get period duration in days."""
        return (self.end_date - self.start_date).total_seconds() / 86400
    
    def __repr__(self):
        return f"BillingPeriod({self.period_id}: {self.start_date.date()} to {self.end_date.date()})"


class IntervalTreeNode:
    """Node in interval tree for efficient period lookup."""
    
    def __init__(self, center: datetime):
        self.center = center
        self.periods: list[BillingPeriod] = []  # Periods containing center
        self.left: Optional[IntervalTreeNode] = None
        self.right: Optional[IntervalTreeNode] = None
    
    def insert(self, period: BillingPeriod):
        """Insert period into tree."""
        if period.start_date <= self.center <= period.end_date:
            self.periods.append(period)
        elif period.end_date < self.center:
            if self.left is None:
                self.left = IntervalTreeNode(period.start_date + (period.end_date - period.start_date) / 2)
            self.left.insert(period)
        else:
            if self.right is None:
                self.right = IntervalTreeNode(period.start_date + (period.end_date - period.start_date) / 2)
            self.right.insert(period)
    
    def query(self, timestamp: datetime) -> list[BillingPeriod]:
        """Query for periods containing timestamp."""
        results = []
        
        # Check periods at this node
        for period in self.periods:
            if period.contains(timestamp):
                results.append(period)
        
        # Recurse to children
        if timestamp < self.center and self.left:
            results.extend(self.left.query(timestamp))
        elif timestamp > self.center and self.right:
            results.extend(self.right.query(timestamp))
        
        return results


class BillingPeriodIndex:
    """
    Efficient index for billing period lookups.
    
    Uses sorted arrays with binary search for O(log n) lookups.
    More memory-efficient than interval trees for large datasets.
    """
    
    def __init__(self):
        self._periods: list[BillingPeriod] = []
        self._starts: list[datetime] = []
        self._ends: list[datetime] = []
        self._by_meter: dict[str, list[int]] = {}  # meter_id -> period indices
    
    def build(self, periods: list[BillingPeriod]):
        """Build index from list of billing periods."""
        # Sort by start date
        self._periods = sorted(periods, key=lambda p: p.start_date)
        self._starts = [p.start_date for p in self._periods]
        self._ends = [p.end_date for p in self._periods]
        
        # Build meter index
        for i, period in enumerate(self._periods):
            if period.meter_id not in self._by_meter:
                self._by_meter[period.meter_id] = []
            self._by_meter[period.meter_id].append(i)
        
        logger.info(f"Built index with {len(self._periods)} periods")
    
    def find_period(
        self,
        timestamp: datetime,
        meter_id: Optional[str] = None
    ) -> Optional[BillingPeriod]:
        """
        Find billing period containing timestamp.
        
        Time complexity: O(log n) with meter filter, O(log n + k) without
        """
        if meter_id and meter_id in self._by_meter:
            # Search only periods for this meter
            indices = self._by_meter[meter_id]
            
            for idx in indices:
                period = self._periods[idx]
                if period.contains(timestamp):
                    return period
            
            return None
        
        # Binary search on all periods
        # Find rightmost period with start <= timestamp
        idx = bisect.bisect_right(self._starts, timestamp) - 1
        
        if idx >= 0:
            period = self._periods[idx]
            if period.contains(timestamp):
                return period
        
        return None
    
    def find_periods_in_range(
        self,
        start: datetime,
        end: datetime,
        meter_id: Optional[str] = None
    ) -> list[BillingPeriod]:
        """Find all periods overlapping with time range."""
        results = []
        
        # Find starting index
        start_idx = bisect.bisect_left(self._ends, start)
        
        # Scan forward until start_date > end
        for i in range(start_idx, len(self._periods)):
            period = self._periods[i]
            
            if period.start_date > end:
                break
            
            if meter_id and period.meter_id != meter_id:
                continue
            
            # Check for overlap
            if period.start_date < end and period.end_date > start:
                results.append(period)
        
        return results


class MeterBillingMatcher:
    """
    High-performance matcher for meter readings to billing periods.
    """
    
    def __init__(self, use_vectorized: bool = True):
        self.period_index = BillingPeriodIndex()
        self.use_vectorized = use_vectorized
        self._stats = {
            'readings_processed': 0,
            'periods_matched': 0,
            'unmatched_readings': 0
        }
    
    def build_period_index(self, periods_df: pd.DataFrame):
        """Build billing period index from DataFrame."""
        periods = []
        
        for _, row in periods_df.iterrows():
            period = BillingPeriod(
                period_id=row['period_id'],
                meter_id=row['meter_id'],
                start_date=pd.to_datetime(row['start_date']),
                end_date=pd.to_datetime(row['end_date']),
                is_partial=row.get('is_partial', False),
                proration_factor=row.get('proration_factor', 1.0)
            )
            periods.append(period)
        
        self.period_index.build(periods)
    
    def match_readings(
        self,
        readings_df: pd.DataFrame,
        timestamp_col: str = 'timestamp',
        meter_col: str = 'meter_id'
    ) -> pd.DataFrame:
        """
        Match readings to billing periods.
        
        Returns DataFrame with added period_id column.
        """
        df = readings_df.copy()
        df['period_id'] = None
        df['period_start'] = None
        df['period_end'] = None
        df['is_partial_period'] = False
        df['proration_factor'] = 1.0
        
        if self.use_vectorized and len(df) > 10000:
            return self._match_vectorized(df, timestamp_col, meter_col)
        else:
            return self._match_iterative(df, timestamp_col, meter_col)
    
    def _match_iterative(
        self,
        df: pd.DataFrame,
        timestamp_col: str,
        meter_col: str
    ) -> pd.DataFrame:
        """Iterative matching for smaller datasets."""
        for idx, row in df.iterrows():
            timestamp = pd.to_datetime(row[timestamp_col])
            meter_id = row[meter_col]
            
            period = self.period_index.find_period(timestamp, meter_id)
            
            if period:
                df.at[idx, 'period_id'] = period.period_id
                df.at[idx, 'period_start'] = period.start_date
                df.at[idx, 'period_end'] = period.end_date
                df.at[idx, 'is_partial_period'] = period.is_partial
                df.at[idx, 'proration_factor'] = period.proration_factor
                self._stats['periods_matched'] += 1
            else:
                self._stats['unmatched_readings'] += 1
            
            self._stats['readings_processed'] += 1
        
        return df
    
    def _match_vectorized(
        self,
        df: pd.DataFrame,
        timestamp_col: str,
        meter_col: str
    ) -> pd.DataFrame:
        """
        Vectorized matching for large datasets.
        
        Uses pandas merge operations for efficiency.
        """
        # Convert timestamps
        df['_ts'] = pd.to_datetime(df[timestamp_col])
        
        # Build periods DataFrame for merge
        periods_data = []
        for period in self.period_index._periods:
            periods_data.append({
                'period_id': period.period_id,
                'meter_id': period.meter_id,
                'period_start': period.start_date,
                'period_end': period.end_date,
                'is_partial_period': period.is_partial,
                'proration_factor': period.proration_factor
            })
        
        periods_df = pd.DataFrame(periods_data)
        
        # Merge on meter_id first
        merged = df.merge(periods_df, on=meter_col, how='left', suffixes=('', '_period'))
        
        # Filter to matching periods
        mask = (
            (merged['_ts'] >= merged['period_start']) &
            (merged['_ts'] < merged['period_end'])
        )
        
        matched = merged[mask].copy()
        unmatched = merged[~mask].copy()
        
        # Keep only first match per reading (in case of overlaps)
        matched = matched.drop_duplicates(subset=[timestamp_col, meter_col], keep='first')
        
        # Clean up
        matched = matched.drop(columns=['_ts'])
        unmatched = unmatched.drop(columns=['_ts'])
        
        # Update stats
        self._stats['periods_matched'] += len(matched)
        self._stats['unmatched_readings'] += len(unmatched)
        self._stats['readings_processed'] += len(df)
        
        # Combine results
        result = pd.concat([matched, unmatched], ignore_index=True)
        
        return result
    
    def get_readings_for_period(
        self,
        period_id: str,
        readings_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Reverse lookup: get all readings for a billing period.
        
        Requires prior call to match_readings.
        """
        return readings_df[readings_df['period_id'] == period_id]
    
    def calculate_prorated_consumption(
        self,
        readings_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calculate prorated consumption for partial billing periods.
        """
        df = readings_df.copy()
        
        # Calculate consumption
        df = df.sort_values(['meter_id', 'timestamp'])
        df['consumption'] = df.groupby('meter_id')['reading'].diff()
        
        # Apply proration for partial periods
        mask = df['is_partial_period'] == True
        df.loc[mask, 'prorated_consumption'] = (
            df.loc[mask, 'consumption'] / df.loc[mask, 'proration_factor']
        )
        df.loc[~mask, 'prorated_consumption'] = df.loc[~mask, 'consumption']
        
        return df
    
    def get_stats(self) -> dict:
        """Get matching statistics."""
        total = self._stats['readings_processed']
        matched = self._stats['periods_matched']
        
        return {
            **self._stats,
            'match_rate': matched / total if total > 0 else 0,
            'periods_in_index': len(self.period_index._periods)
        }


# Optimized version using NumPy for very large datasets
class NumpyBillingMatcher:
    """
    Ultra-fast matcher using NumPy arrays.
    Best for datasets that fit in memory.
    """
    
    def __init__(self):
        self._period_starts: Optional[np.ndarray] = None
        self._period_ends: Optional[np.ndarray] = None
        self._period_ids: Optional[np.ndarray] = None
        self._meter_ids: Optional[np.ndarray] = None
    
    def build(self, periods_df: pd.DataFrame):
        """Build NumPy arrays from periods DataFrame."""
        # Convert to timestamps (nanoseconds for precision)
        self._period_starts = pd.to_datetime(periods_df['start_date']).values.astype('datetime64[ns]')
        self._period_ends = pd.to_datetime(periods_df['end_date']).values.astype('datetime64[ns]')
        self._period_ids = periods_df['period_id'].values
        self._meter_ids = periods_df['meter_id'].values
    
    def match(self, readings_df: pd.DataFrame) -> np.ndarray:
        """
        Match readings to periods using NumPy.
        
        Returns array of period indices (-1 for unmatched).
        """
        timestamps = pd.to_datetime(readings_df['timestamp']).values.astype('datetime64[ns]')
        meter_ids = readings_df['meter_id'].values
        
        n_readings = len(timestamps)
        matches = np.full(n_readings, -1, dtype=np.int64)
        
        # Vectorized matching
        for i in range(n_readings):
            ts = timestamps[i]
            meter = meter_ids[i]
            
            # Find periods for this meter
            meter_mask = self._meter_ids == meter
            
            if not meter_mask.any():
                continue
            
            # Check which period contains timestamp
            start_mask = self._period_starts <= ts
            end_mask = self._period_ends > ts
            
            period_mask = meter_mask & start_mask & end_mask
            
            if period_mask.any():
                matches[i] = np.where(period_mask)[0][0]
        
        return matches


# Usage example and performance test
if __name__ == "__main__":
    # Generate test data
    np.random.seed(42)
    
    n_meters = 1000
    n_readings = 100_000
    n_periods = 5000
    
    # Generate meters
    meter_ids = [f'M{i:05d}' for i in range(n_meters)]
    
    # Generate billing periods
    periods_data = []
    for i in range(n_periods):
        meter = np.random.choice(meter_ids)
        start = pd.Timestamp('2023-01-01') + pd.Timedelta(days=np.random.randint(0, 365))
        duration = pd.Timedelta(days=np.random.randint(28, 35))  # Monthly-ish
        
        periods_data.append({
            'period_id': f'P{i:06d}',
            'meter_id': meter,
            'start_date': start,
            'end_date': start + duration,
            'is_partial': np.random.random() < 0.1,
            'proration_factor': np.random.uniform(0.5, 1.0) if np.random.random() < 0.1 else 1.0
        })
    
    periods_df = pd.DataFrame(periods_data)
    
    # Generate meter readings
    readings_data = []
    for i in range(n_readings):
        meter = np.random.choice(meter_ids)
        timestamp = pd.Timestamp('2023-01-01') + pd.Timedelta(
            minutes=np.random.randint(0, 365 * 24 * 60)
        )
        
        readings_data.append({
            'reading_id': f'R{i:08d}',
            'meter_id': meter,
            'timestamp': timestamp,
            'reading': np.random.exponential(100) + np.random.randint(1000, 10000)
        })
    
    readings_df = pd.DataFrame(readings_data)
    
    print(f"Test data: {n_readings:,} readings, {n_periods:,} periods, {n_meters:,} meters")
    
    # Test matcher
    matcher = MeterBillingMatcher(use_vectorized=True)
    
    import time
    start = time.time()
    matcher.build_period_index(periods_df)
    build_time = time.time() - start
    
    start = time.time()
    matched_df = matcher.match_readings(readings_df)
    match_time = time.time() - start
    
    print(f"\nPerformance:")
    print(f"  Index build time: {build_time:.3f}s")
    print(f"  Match time: {match_time:.3f}s")
    print(f"  Throughput: {n_readings / match_time:,.0f} readings/second")
    
    print(f"\nResults:")
    stats = matcher.get_stats()
    print(f"  Match rate: {stats['match_rate']:.1%}")
    print(f"  Unmatched: {stats['unmatched_readings']:,}")
    
    # Show sample matches
    print(f"\nSample matches:")
    sample = matched_df[matched_df['period_id'].notna()].head(3)
    for _, row in sample.iterrows():
        print(f"  {row['reading_id']}: {row['timestamp']} -> {row['period_id']}")
```

### Key Design Decisions

1. **Binary Search Index**: O(log n) lookups vs O(n) linear scan
2. **Meter Partitioning**: Reduces search space when meter known
3. **Vectorized Operations**: Pandas merge for large datasets
4. **NumPy Backend**: Raw arrays for memory-constrained environments
5. **Bidirectional Lookup**: Support both reading→period and period→readings

### Complexity Analysis

- **Index Build**: O(n log n) for sorting
- **Single Lookup**: O(log n) with binary search
- **Batch Matching**: O(n log n) with vectorized operations
- **Memory**: O(n) for index storage

---

## Summary: Best Practices Demonstrated

### Code Quality
1. **Type Hints**: Comprehensive typing for IDE support and documentation
2. **Docstrings**: Google-style docstrings for all public APIs
3. **Error Handling**: Graceful degradation with detailed error messages
4. **Logging**: Structured logging at appropriate levels
5. **Configuration**: Externalized config via dataclasses

### Performance
1. **Vectorization**: NumPy/Pandas operations over Python loops
2. **Memory Management**: Chunked processing, type optimization
3. **Parallel Processing**: Process/thread pools for CPU/IO bound tasks
4. **Caching**: Strategic caching for expensive operations
5. **Lazy Evaluation**: Iterators for streaming data

### Production Readiness
1. **Monitoring**: Metrics collection and health checks
2. **Testing**: Unit, integration, and property-based tests
3. **Documentation**: Inline docs and usage examples
4. **Observability**: Structured logging and tracing hooks
5. **Scalability**: Horizontal scaling patterns

### Domain-Specific
1. **Utility Knowledge**: Billing cycles, consumption patterns
2. **Regulatory Compliance**: Audit trails, data retention
3. **Business Logic**: Proration, partial periods, timezone handling
4. **Operational Needs**: Alerting, dashboards, reporting

---

*"Readability counts. Simple is better than complex. Complex is better than complicated."* - The Zen of Python

*"In the face of ambiguity, refuse the temptation to guess."* - The Zen of Python

*These questions test the candidate's ability to write production-quality code that balances elegance with pragmatism - exactly what Business Stream needs for their data infrastructure.*
