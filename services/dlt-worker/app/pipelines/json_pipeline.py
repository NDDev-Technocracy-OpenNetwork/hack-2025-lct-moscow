import dlt
import json
import pandas as pd
import os
from typing import Iterator, Dict, Any, List
from .base_pipeline import BasePipeline


class JSONPipeline(BasePipeline):
    """Pipeline for processing JSON files"""

    def __init__(self, job_config: Dict[str, Any]):
        super().__init__(job_config)
        self.chunk_size = job_config.get('chunk_size', 10000)
        self.file_path = f"/app/data/input/{self.filename}"
        self.total_records = self.estimate_total_records(self.file_path)

    def create_pipeline(self) -> dlt.Pipeline:
        """Create dlt pipeline based on destination"""
        return self._create_pipeline_by_destination()

    def create_resource(self):
        """Create dlt resource for JSON data"""
        return self._json_data_resource()

    @dlt.resource(table_name=lambda: dlt.config.get("table_name", "json_data"))
    def _json_data_resource(self) -> Iterator[List[Dict]]:
        """DLT resource for reading JSON file"""

        try:
            self.update_progress(0, "analyzing", "Analyzing JSON file structure")

            # Check if file is JSONL (JSON Lines) or regular JSON
            if self._is_jsonl_format():
                self.logger.info("Processing JSONL format file")
                yield from self._read_jsonl()
            else:
                self.logger.info("Processing JSON array/object file")
                yield from self._read_json_array()

        except Exception as e:
            self.logger.error(f"Error reading JSON file: {str(e)}")
            raise

    def _is_jsonl_format(self) -> bool:
        """Check if file is JSONL (JSON Lines) format"""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                if not first_line:
                    return False

                # Try to parse first line as JSON object
                json.loads(first_line)

                # Check if second line is also a valid JSON object
                second_line = f.readline().strip()
                if second_line:
                    json.loads(second_line)
                    return True  # Two valid JSON objects = JSONL

                return False  # Only one line, treat as regular JSON

        except json.JSONDecodeError:
            return False
        except Exception:
            return False

    def _read_jsonl(self) -> Iterator[List[Dict]]:
        """Read JSONL file in chunks"""
        batch = []
        line_count = 0

        with open(self.file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)
                    batch.append(self._flatten_json_object(data))
                    line_count += 1

                    if len(batch) >= self.chunk_size:
                        self.update_progress(
                            len(batch),
                            f"processing_batch_{line_num // self.chunk_size}",
                            f"Processed batch with {len(batch)} records"
                        )
                        yield batch
                        batch = []

                except json.JSONDecodeError as e:
                    self.logger.warning(f"Skipping invalid JSON on line {line_num}: {e}")
                    continue

        # Yield final batch
        if batch:
            self.update_progress(
                len(batch),
                "processing_final_batch",
                f"Processed final batch with {len(batch)} records"
            )
            yield batch

        self.logger.info(f"JSONL processing completed. Total records: {line_count}")

    def _read_json_array(self) -> Iterator[List[Dict]]:
        """Read JSON array or single object"""
        with open(self.file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if isinstance(data, list):
            # Process array in chunks
            total_records = len(data)
            self.logger.info(f"Processing JSON array with {total_records} records")

            for i in range(0, total_records, self.chunk_size):
                chunk = data[i:i + self.chunk_size]
                processed_chunk = [self._flatten_json_object(item) for item in chunk]

                self.update_progress(
                    len(processed_chunk),
                    f"processing_chunk_{i // self.chunk_size + 1}",
                    f"Processed chunk with {len(processed_chunk)} records"
                )
                yield processed_chunk

        else:
            # Single object
            self.logger.info("Processing single JSON object")
            flattened = self._flatten_json_object(data)
            self.update_progress(1, "processing_single_object", "Processed single JSON object")
            yield [flattened]

    def _flatten_json_object(self, obj: Any, parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """Flatten nested JSON object"""
        if not isinstance(obj, dict):
            return {'value': obj} if parent_key == '' else obj

        items = []
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k

            if isinstance(v, dict):
                items.extend(self._flatten_json_object(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Handle arrays
                if v and isinstance(v[0], dict):
                    # Array of objects - take first as representative
                    items.extend(self._flatten_json_object(v[0], new_key, sep=sep).items())
                else:
                    # Array of primitives - convert to string
                    items.append((new_key, str(v)))
            else:
                items.append((new_key, v))

        return dict(items)

    def estimate_total_records(self, file_path: str) -> int:
        """Estimate total number of records in JSON file"""
        if not os.path.exists(file_path):
            return 0

        try:
            file_size = os.path.getsize(file_path)

            # For small files, count precisely
            if file_size < 10 * 1024 * 1024:  # 10MB
                if self._is_jsonl_format():
                    with open(file_path, 'r', encoding='utf-8') as f:
                        return sum(1 for line in f if line.strip())
                else:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        return len(data) if isinstance(data, list) else 1

            # For large files, estimate based on file size
            # Rough estimate: average 100 bytes per JSON record
            estimated = file_size // 100
            self.logger.info(f"Estimated {estimated} records in JSON file (file size: {file_size} bytes)")
            return estimated

        except Exception as e:
            self.logger.warning(f"Could not estimate record count: {e}")
            return 0