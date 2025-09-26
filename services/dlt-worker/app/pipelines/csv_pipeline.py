import dlt
import pandas as pd
import csv
import chardet
import os
from typing import Generator, Dict, Any, Iterator
from .base_pipeline import BasePipeline


class CSVPipeline(BasePipeline):
    """Pipeline for processing CSV files"""

    def __init__(self, job_config: Dict[str, Any]):
        super().__init__(job_config)
        self.chunk_size = job_config.get('chunk_size', 10000)
        self.file_path = f"/app/data/input/{self.filename}"
        self.encoding = None
        self.separator = None

        # Estimate total records for progress tracking
        self.total_records = self.estimate_total_records(self.file_path)

    def create_pipeline(self) -> dlt.Pipeline:
        """Create dlt pipeline based on destination"""
        return self._create_pipeline_by_destination()

    def create_resource(self):
        """Create dlt resource for CSV data"""
        return self._csv_data_resource()

    @dlt.resource(table_name=lambda: dlt.config.get("table_name", "csv_data"))
    def _csv_data_resource(self) -> Iterator[pd.DataFrame]:
        """DLT resource for reading CSV file in chunks"""

        try:
            # Detect file parameters
            self.encoding = self._detect_encoding()
            self.separator = self._detect_separator()

            self.logger.info(f"Reading CSV with encoding={self.encoding}, separator='{self.separator}'")
            self.update_progress(0, "analyzing", f"File analysis complete. Estimated {self.total_records} records")

            # Read file in chunks
            chunk_count = 0
            total_processed = 0

            chunk_reader = pd.read_csv(
                self.file_path,
                chunksize=self.chunk_size,
                encoding=self.encoding,
                sep=self.separator,
                low_memory=False,
                na_values=['', 'NULL', 'null', 'NA', 'n/a', 'N/A', '#N/A', 'N/A', 'n/a', '#N/A', '#NULL!', '#REF!', '#VALUE!'],
                keep_default_na=True,
                skip_blank_lines=True
            )

            for chunk in chunk_reader:
                chunk_count += 1

                # Clean the dataframe
                chunk = self._clean_dataframe(chunk)

                if len(chunk) > 0:  # Only yield non-empty chunks
                    # Update progress
                    total_processed += len(chunk)
                    self.update_progress(
                        len(chunk),
                        f"processing_chunk_{chunk_count}",
                        f"Processed chunk {chunk_count} with {len(chunk)} records"
                    )

                    self.logger.debug(f"Yielding chunk {chunk_count} with {len(chunk)} records")
                    yield chunk

            self.logger.info(f"CSV processing completed. Total chunks: {chunk_count}, Total records: {total_processed}")

        except Exception as e:
            self.logger.error(f"Error reading CSV file: {str(e)}")
            raise

    def _detect_encoding(self) -> str:
        """Automatically detect file encoding"""
        try:
            with open(self.file_path, 'rb') as f:
                sample = f.read(100000)  # Read first 100KB
                result = chardet.detect(sample)
                encoding = result['encoding'] or 'utf-8'
                confidence = result['confidence'] or 0

                self.logger.info(f"Detected encoding: {encoding} (confidence: {confidence:.2f})")
                return encoding
        except Exception as e:
            self.logger.warning(f"Encoding detection failed: {e}. Using utf-8")
            return 'utf-8'

    def _detect_separator(self) -> str:
        """Automatically detect CSV separator"""
        try:
            with open(self.file_path, 'r', encoding=self.encoding or 'utf-8') as f:
                sample = f.read(8192)  # Read first 8KB

                # Try to detect delimiter
                sniffer = csv.Sniffer()
                dialect = sniffer.sniff(sample, delimiters=',;\t|')
                delimiter = dialect.delimiter

                self.logger.info(f"Detected CSV delimiter: '{delimiter}'")
                return delimiter

        except Exception as e:
            self.logger.warning(f"Delimiter detection failed: {e}. Using comma")
            return ','

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare dataframe"""

        # Remove completely empty rows
        df = df.dropna(how='all')

        if len(df) == 0:
            return df

        # Clean column names
        df.columns = (df.columns.astype(str)
                     .str.strip()
                     .str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)
                     .str.replace(r'_+', '_', regex=True)
                     .str.strip('_')
                     .str.lower())

        # Handle duplicate column names
        cols = df.columns.tolist()
        seen = set()
        for i, col in enumerate(cols):
            original_col = col
            counter = 1
            while col in seen:
                col = f"{original_col}_{counter}"
                counter += 1
            seen.add(col)
            cols[i] = col
        df.columns = cols

        # Data type conversion and cleaning
        for col in df.columns:
            if df[col].dtype == 'object':
                # Clean string data
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(['nan', 'None', 'NaN', '<NA>'], None)

                # Try to convert to more specific types
                if self._is_datetime_column(df[col]):
                    df[col] = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
                elif self._is_numeric_column(df[col]):
                    # Try integer first, then float
                    numeric_series = pd.to_numeric(df[col], errors='coerce')
                    if numeric_series.equals(numeric_series.astype('Int64', errors='ignore')):
                        df[col] = numeric_series.astype('Int64')  # Nullable integer
                    else:
                        df[col] = numeric_series
                elif self._is_boolean_column(df[col]):
                    df[col] = self._convert_to_boolean(df[col])

        return df

    def _is_datetime_column(self, series: pd.Series) -> bool:
        """Check if column contains datetime data"""
        if len(series.dropna()) == 0:
            return False

        sample = series.dropna().head(100)

        try:
            # Try to parse a sample
            parsed = pd.to_datetime(sample, errors='raise', infer_datetime_format=True)
            # If more than 80% are valid dates, consider it datetime
            valid_count = parsed.notna().sum()
            return (valid_count / len(sample)) > 0.8
        except:
            return False

    def _is_numeric_column(self, series: pd.Series) -> bool:
        """Check if column contains numeric data"""
        if len(series.dropna()) == 0:
            return False

        sample = series.dropna().head(100)

        try:
            pd.to_numeric(sample, errors='raise')
            return True
        except:
            return False

    def _is_boolean_column(self, series: pd.Series) -> bool:
        """Check if column contains boolean data"""
        if len(series.dropna()) == 0:
            return False

        sample = series.dropna().str.lower().head(100)
        boolean_values = {'true', 'false', 'yes', 'no', '1', '0', 'y', 'n', 't', 'f'}

        # If more than 80% of values are boolean-like, consider it boolean
        boolean_count = sample.isin(boolean_values).sum()
        return (boolean_count / len(sample)) > 0.8

    def _convert_to_boolean(self, series: pd.Series) -> pd.Series:
        """Convert string series to boolean"""
        series_lower = series.str.lower()

        true_values = {'true', 'yes', '1', 'y', 't', 'on'}
        false_values = {'false', 'no', '0', 'n', 'f', 'off'}

        result = series.copy()
        result[series_lower.isin(true_values)] = True
        result[series_lower.isin(false_values)] = False
        result[~series_lower.isin(true_values | false_values)] = None

        return result.astype('boolean')  # Nullable boolean

    def estimate_total_records(self, file_path: str) -> int:
        """Estimate total number of records in CSV file"""
        if not os.path.exists(file_path):
            return 0

        try:
            # Quick estimation by counting lines
            with open(file_path, 'rb') as f:
                lines = sum(1 for _ in f)

            # Subtract header line
            estimated_records = max(0, lines - 1)
            self.logger.info(f"Estimated {estimated_records} records in CSV file")
            return estimated_records

        except Exception as e:
            self.logger.warning(f"Could not estimate record count: {e}")
            return 0