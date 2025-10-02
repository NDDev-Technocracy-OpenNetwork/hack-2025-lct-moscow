import pandas as pd
import json
import xml.etree.ElementTree as ET
import xmltodict
import chardet
import os
from typing import Dict, List, Any, Optional, Tuple
import logging
from pathlib import Path
# import magic  # Removed due to system dependency issues
import csv

from app.schemas.files import ColumnInfo

logger = logging.getLogger(__name__)


class FileProcessor:
    """File processing and analysis for different file formats"""

    def __init__(self, sample_size: int = 1000):
        self.sample_size = sample_size

    async def analyze_file(self, file_path: str, file_format: str) -> Dict[str, Any]:
        """Analyze file and extract schema information"""

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        file_size = os.path.getsize(file_path)
        encoding = self._detect_encoding(file_path)

        schema_info = {
            "file_path": file_path,
            "file_size": file_size,
            "encoding": encoding,
            "file_format": file_format,
            "columns": [],
            "sample_data": [],
            "total_rows": None,
            "analysis_timestamp": pd.Timestamp.now().isoformat(),
            "mime_type": self._get_mime_type(file_path)
        }

        try:
            if file_format.lower() == 'csv':
                schema_info.update(await self._analyze_csv(file_path, encoding))
            elif file_format.lower() == 'json':
                schema_info.update(await self._analyze_json(file_path, encoding))
            elif file_format.lower() == 'xml':
                schema_info.update(await self._analyze_xml(file_path, encoding))
            else:
                raise ValueError(f"Unsupported file format: {file_format}")

            logger.info(f"File analysis completed: {file_path}, columns: {len(schema_info['columns'])}")

        except Exception as e:
            logger.error(f"Error analyzing file {file_path}: {e}")
            schema_info['error'] = str(e)
            raise

        return schema_info

    def _detect_encoding(self, file_path: str) -> str:
        """Detect file encoding"""
        try:
            with open(file_path, 'rb') as file:
                raw_data = file.read(10000)  # Read first 10KB
                encoding_info = chardet.detect(raw_data)
                return encoding_info.get('encoding', 'utf-8') or 'utf-8'
        except Exception:
            return 'utf-8'

    def _get_mime_type(self, file_path: str) -> str:
        """Get MIME type of the file"""
        # Simplified fallback without python-magic
        file_path = Path(file_path)
        if file_path.suffix.lower() == '.csv':
            return 'text/csv'
        elif file_path.suffix.lower() == '.json':
            return 'application/json'
        elif file_path.suffix.lower() in ['.xml', '.xsd']:
            return 'application/xml'
        else:
            return 'application/octet-stream'

    async def _analyze_csv(self, file_path: str, encoding: str) -> Dict[str, Any]:
        """Analyze CSV file structure with optimizations for large files"""

        logger.info(f"Starting CSV analysis for: {file_path}")

        # First, detect delimiter and other CSV parameters
        delimiter, quotechar = self._detect_csv_params(file_path, encoding)

        # Initialize variables
        total_rows = None

        # Read sample for analysis
        try:
            df_sample = pd.read_csv(
                file_path,
                encoding=encoding,
                nrows=self.sample_size,
                delimiter=delimiter,
                quotechar=quotechar,
                low_memory=False
            )

            # Get total row count (approximately) - this can be slow for large files
            logger.info(f"Starting row count estimation...")
            total_rows = self._count_csv_rows(file_path, encoding, delimiter)
            logger.info(f"CSV analysis: sample rows={len(df_sample)}, total_rows={total_rows}")

        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            try:
                # Try with different parameters
                df_sample = pd.read_csv(file_path, encoding=encoding, nrows=self.sample_size)
                total_rows = None
                logger.info(f"CSV fallback: sample rows={len(df_sample)}, total_rows=None")
            except Exception as e2:
                logger.error(f"CSV fallback also failed: {e2}")
                raise e2

        columns = []
        sample_data = []

        try:
            logger.info(f"Analyzing {len(df_sample.columns)} columns...")
            for i, col in enumerate(df_sample.columns):
                col_info = self._analyze_column(df_sample[col], col)
                columns.append(col_info)
                if (i + 1) % 10 == 0:  # Log progress every 10 columns
                    logger.info(f"Analyzed {i + 1}/{len(df_sample.columns)} columns")
            logger.info(f"Analyzed {len(columns)} columns successfully")
        except Exception as e:
            logger.error(f"Error analyzing columns: {e}")
            raise e

        try:
            # Get sample data (first 10 rows) and clean NaN values IMMEDIATELY
            df_subset = df_sample.head(10)
            # Replace NaN values manually using numpy
            import numpy as np
            df_clean = df_subset.replace([np.nan, np.inf, -np.inf], None)
            sample_data = df_clean.to_dict('records')
            logger.info(f"Generated {len(sample_data)} clean sample records (NaN->None)")
        except Exception as e:
            logger.error(f"Error generating sample data: {e}")
            raise e

        return {
            "columns": columns,
            "sample_data": sample_data,
            "total_rows": total_rows,
            "csv_delimiter": delimiter,
            "csv_quotechar": quotechar
        }

    async def _analyze_json(self, file_path: str, encoding: str) -> Dict[str, Any]:
        """Analyze JSON file structure"""

        with open(file_path, 'r', encoding=encoding) as file:
            # Try to determine if it's a single JSON object or JSON lines
            first_line = file.readline().strip()
            file.seek(0)

            if first_line.startswith('['):
                # JSON array - use ijson for memory-efficient parsing of large files
                import ijson
                sample_data = []
                total_rows = 0

                try:
                    # Stream parse JSON array without loading entire file
                    for item in ijson.items(file, 'item'):
                        if len(sample_data) < self.sample_size:
                            sample_data.append(item)
                        total_rows += 1
                except Exception as ijson_error:
                    logger.warning(f"ijson parsing failed: {ijson_error}, falling back to json.load")
                    # Fallback to standard json.load if ijson fails
                    file.seek(0)
                    try:
                        data = json.load(file)
                        if isinstance(data, list):
                            sample_data = data[:self.sample_size] if len(data) > self.sample_size else data
                            total_rows = len(data)
                        else:
                            sample_data = [data]
                            total_rows = 1
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON parsing failed completely: {e}")
                        raise
            else:
                # JSON lines or single object
                lines = file.readlines()
                sample_data = []

                for i, line in enumerate(lines[:self.sample_size]):
                    try:
                        obj = json.loads(line.strip())
                        sample_data.append(obj)
                    except json.JSONDecodeError:
                        continue

                total_rows = len(lines)

        if not sample_data:
            raise ValueError("No valid JSON objects found in file")

        # Flatten and analyze structure
        columns = self._extract_json_schema(sample_data)

        return {
            "columns": columns,
            "sample_data": sample_data[:10],
            "total_rows": total_rows,
            "json_structure": "array" if first_line.startswith('[') else "lines"
        }

    async def _analyze_xml(self, file_path: str, encoding: str) -> Dict[str, Any]:
        """Analyze XML file structure with streaming for large files"""

        file_size = os.path.getsize(file_path)
        is_large_file = file_size > 50 * 1024 * 1024  # 50MB threshold

        if is_large_file:
            logger.info(f"Large XML file detected ({file_size / (1024*1024):.1f}MB), using streaming parser")
            return await self._analyze_xml_streaming(file_path, encoding)

        # For small files, use the original method
        with open(file_path, 'r', encoding=encoding) as file:
            content = file.read()

        # Parse XML
        try:
            root = ET.fromstring(content)
        except ET.ParseError as e:
            raise ValueError(f"Invalid XML format: {e}")

        # Convert to dictionary for easier analysis
        xml_dict = xmltodict.parse(content)

        # Find repeating elements (likely records)
        records = self._find_xml_records(root)
        sample_data = records[:self.sample_size] if len(records) > self.sample_size else records

        # Extract schema from sample records
        columns = self._extract_xml_schema(sample_data)

        return {
            "columns": columns,
            "sample_data": sample_data[:10],
            "total_rows": len(records),
            "xml_root_element": root.tag,
            "xml_structure": self._describe_xml_structure(root)
        }

    async def _analyze_xml_streaming(self, file_path: str, encoding: str) -> Dict[str, Any]:
        """Analyze large XML files using streaming parser"""
        import xml.etree.ElementTree as ET

        logger.info(f"Starting streaming XML analysis for file: {file_path}")
        records = []
        root_tag = None
        record_tag = None
        total_elements = 0

        # Use iterparse for memory-efficient parsing (pass file path as string, not file handle)
        try:
            context = ET.iterparse(file_path, events=('start', 'end'))
            context = iter(context)
        except Exception as e:
            logger.error(f"Failed to create iterparse context: {e}")
            raise ValueError(f"Cannot parse XML file: {e}")

        try:
            event, root = next(context)
            root_tag = root.tag
            logger.info(f"XML root element: {root_tag}")

            # Find first repeating child element
            for event, elem in context:
                if event == 'end':
                    if elem.tag != root_tag and record_tag is None:
                        # First child element becomes the record pattern
                        record_tag = elem.tag
                        logger.info(f"Detected record element: {record_tag}")

                    if elem.tag == record_tag:
                        total_elements += 1

                        # Only collect sample records
                        if len(records) < self.sample_size:
                            record = self._xml_element_to_dict(elem)
                            records.append(record)

                            if len(records) % 100 == 0:
                                logger.info(f"Collected {len(records)} sample records...")

                        # Clear element to free memory
                        elem.clear()

                        # Stop after collecting enough samples
                        if len(records) >= self.sample_size:
                            # Continue counting only
                            for event, elem in context:
                                if event == 'end' and elem.tag == record_tag:
                                    total_elements += 1
                                    elem.clear()

                                    if total_elements % 1000 == 0:
                                        logger.info(f"Counted {total_elements} total records...")
                            break

            logger.info(f"XML analysis complete: {len(records)} samples, {total_elements} total records")

            # Extract schema from sample records
            columns = self._extract_xml_schema(records[:self.sample_size])

            return {
                "columns": columns,
                "sample_data": records[:10],
                "total_rows": total_elements,
                "xml_root_element": root_tag,
                "xml_structure": f"Root: {root_tag}, Records: {record_tag}, Count: {total_elements}"
            }

        except Exception as e:
            logger.error(f"Error in streaming XML analysis: {e}")
            raise ValueError(f"Failed to parse XML file: {e}")

    def _detect_csv_params(self, file_path: str, encoding: str) -> Tuple[str, str]:
        """Detect CSV delimiter and quote character"""
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                sample = file.read(8192)
                sniffer = csv.Sniffer()
                dialect = sniffer.sniff(sample, delimiters=',;\t|')
                return dialect.delimiter, dialect.quotechar
        except Exception:
            return ',', '"'

    def _count_csv_rows(self, file_path: str, encoding: str, delimiter: str) -> Optional[int]:
        """Count total rows in CSV file efficiently (with fast estimation for large files)"""
        try:
            file_size = os.path.getsize(file_path)
            logger.info(f"Counting rows for file size: {file_size / (1024*1024):.1f}MB")

            # For very large files (> 100MB), use fast estimation
            if file_size > 100 * 1024 * 1024:
                logger.info(f"Large file detected, using fast estimation")
                with open(file_path, 'r', encoding=encoding) as file:
                    # Read just a small sample to estimate
                    sample_size = 8192  # 8KB sample
                    header = file.readline()
                    sample = file.read(sample_size)

                    if not sample:
                        return 1

                    # Simple line count in sample
                    lines_in_sample = sample.count('\n')
                    if lines_in_sample > 0:
                        # Rough estimation: file_size / average_line_size
                        avg_line_size = len(sample) / lines_in_sample
                        estimated_rows = int(file_size / avg_line_size)
                        logger.info(f"Fast estimation: ~{estimated_rows} rows")
                        return max(0, estimated_rows)
                    return 1000  # Default fallback

            # For medium files (10MB - 100MB), use moderate estimation
            elif file_size > 10 * 1024 * 1024:
                logger.info(f"Medium file detected, using moderate estimation")
                with open(file_path, 'r', encoding=encoding) as file:
                    # Count first 1000 lines to get average line size
                    line_count = 0
                    bytes_read = 0

                    for line in file:
                        line_count += 1
                        bytes_read += len(line.encode(encoding))
                        if line_count >= 1000:
                            break

                    if line_count > 0:
                        avg_line_size = bytes_read / line_count
                        estimated_rows = int(file_size / avg_line_size) - 1  # Subtract header
                        logger.info(f"Moderate estimation: ~{estimated_rows} rows")
                        return max(0, estimated_rows)
                    return 1000  # Default fallback

            # For small files (< 10MB), count exactly
            else:
                logger.info(f"Small file detected, counting exactly")
                with open(file_path, 'r', encoding=encoding) as file:
                    reader = csv.reader(file, delimiter=delimiter)
                    row_count = sum(1 for row in reader) - 1  # Subtract header
                    logger.info(f"Exact count: {row_count} rows")
                    return max(0, row_count)

        except Exception as e:
            logger.warning(f"Could not count rows, using fallback: {e}")
            # Return a reasonable default based on file size
            try:
                file_size = os.path.getsize(file_path)
                fallback_estimate = max(1000, int(file_size / 200))  # Assume ~200 bytes per row
                logger.info(f"Fallback estimation: {fallback_estimate} rows")
                return fallback_estimate
            except:
                return 10000  # Final fallback

    def _analyze_column(self, series: pd.Series, column_name: str) -> ColumnInfo:
        """Analyze individual column characteristics"""

        # Basic stats
        total_count = len(series)
        null_count = series.isnull().sum()
        non_null_count = total_count - null_count

        # Data type inference
        data_type = self._infer_data_type(series)

        # Sample values (non-null)
        sample_values = series.dropna().head(5).astype(str).tolist()

        # Max length for string columns
        max_length = None
        if data_type in ['VARCHAR', 'TEXT']:
            max_length = series.astype(str).str.len().max() if non_null_count > 0 else 0

        return ColumnInfo(
            name=column_name,
            data_type=data_type,
            nullable=null_count > 0,
            max_length=max_length,
            sample_values=sample_values
        )

    def _infer_data_type(self, series: pd.Series) -> str:
        """Infer SQL data type from pandas series"""

        if series.dtype == 'object':
            # Check if it's numeric but stored as string
            try:
                pd.to_numeric(series.dropna())
                if series.dropna().astype(str).str.contains(r'\.').any():
                    return 'DECIMAL'
                else:
                    return 'BIGINT'
            except (ValueError, TypeError):
                pass

            # Check if it's datetime
            try:
                pd.to_datetime(series.dropna())
                return 'TIMESTAMP'
            except (ValueError, TypeError):
                pass

            # String data
            max_len = series.astype(str).str.len().max()
            if max_len <= 255:
                return 'VARCHAR'
            else:
                return 'TEXT'

        elif series.dtype in ['int64', 'int32', 'int16', 'int8']:
            return 'BIGINT'
        elif series.dtype in ['float64', 'float32']:
            return 'DECIMAL'
        elif series.dtype == 'bool':
            return 'BOOLEAN'
        elif series.dtype.name.startswith('datetime'):
            return 'TIMESTAMP'
        else:
            return 'TEXT'

    def _extract_json_schema(self, json_data: List[Dict]) -> List[ColumnInfo]:
        """Extract schema from JSON data"""

        all_keys = set()
        key_types = {}
        key_samples = {}

        for record in json_data:
            if isinstance(record, dict):
                for key, value in record.items():
                    all_keys.add(key)

                    # Track data types
                    if key not in key_types:
                        key_types[key] = set()
                    key_types[key].add(type(value).__name__)

                    # Track sample values
                    if key not in key_samples:
                        key_samples[key] = []
                    if len(key_samples[key]) < 5 and value is not None:
                        key_samples[key].append(str(value))

        columns = []
        for key in sorted(all_keys):
            data_type = self._json_type_to_sql(key_types.get(key, {'str'}))

            columns.append(ColumnInfo(
                name=key,
                data_type=data_type,
                nullable=True,  # JSON fields are generally nullable
                sample_values=key_samples.get(key, [])
            ))

        return columns

    def _json_type_to_sql(self, types: set) -> str:
        """Convert JSON/Python types to SQL types"""

        if 'int' in types and 'float' not in types:
            return 'BIGINT'
        elif 'float' in types or ('int' in types and 'float' in types):
            return 'DECIMAL'
        elif 'bool' in types and len(types) == 1:
            return 'BOOLEAN'
        elif 'list' in types or 'dict' in types:
            return 'JSON'
        else:
            return 'TEXT'

    def _find_xml_records(self, root) -> List[Dict]:
        """Find repeating XML elements that represent records"""

        records = []

        # Look for repeating child elements
        child_tags = {}
        for child in root:
            tag = child.tag
            if tag not in child_tags:
                child_tags[tag] = []
            child_tags[tag].append(child)

        # Find the most common repeating element
        max_count = 0
        record_elements = None

        for tag, elements in child_tags.items():
            if len(elements) > max_count:
                max_count = len(elements)
                record_elements = elements

        # Convert XML elements to dictionaries
        if record_elements:
            for element in record_elements:
                record = self._xml_element_to_dict(element)
                records.append(record)

        return records

    def _xml_element_to_dict(self, element) -> Dict:
        """Convert XML element to dictionary"""
        result = {}

        # Add attributes
        if element.attrib:
            for key, value in element.attrib.items():
                result[f"@{key}"] = value

        # Add text content
        if element.text and element.text.strip():
            if len(list(element)) == 0:  # Leaf element
                result['text'] = element.text.strip()
            else:
                result['#text'] = element.text.strip()

        # Add child elements
        for child in element:
            child_data = self._xml_element_to_dict(child)
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data

        return result

    def _extract_xml_schema(self, records: List[Dict]) -> List[ColumnInfo]:
        """Extract schema from XML records"""

        # Flatten nested dictionaries
        flattened_records = []
        for record in records:
            flattened = self._flatten_dict(record)
            flattened_records.append(flattened)

        # Use the same logic as JSON schema extraction
        return self._extract_json_schema(flattened_records)

    def _flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """Flatten nested dictionary"""
        items = []

        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k

            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                if v and isinstance(v[0], dict):
                    # Take the first item as representative
                    items.extend(self._flatten_dict(v[0], new_key, sep=sep).items())
                else:
                    items.append((new_key, str(v)))
            else:
                items.append((new_key, v))

        return dict(items)

    def _describe_xml_structure(self, root) -> Dict[str, Any]:
        """Describe XML file structure"""

        def analyze_element(element, depth=0):
            info = {
                "tag": element.tag,
                "depth": depth,
                "has_attributes": bool(element.attrib),
                "has_text": bool(element.text and element.text.strip()),
                "children": []
            }

            child_counts = {}
            for child in element:
                tag = child.tag
                child_counts[tag] = child_counts.get(tag, 0) + 1

                if len(info["children"]) < 5:  # Limit to avoid huge structures
                    info["children"].append(analyze_element(child, depth + 1))

            info["child_counts"] = child_counts
            return info

        return analyze_element(root)