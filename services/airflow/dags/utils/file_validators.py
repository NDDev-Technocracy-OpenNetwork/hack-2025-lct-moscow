from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
import os
import magic
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class FileValidatorOperator(BaseOperator):
    """
    Operator for validating uploaded files before processing
    """

    def __init__(self, job_config: Dict[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_config = job_config

    def execute(self, context: Context):
        """Validate file integrity and format"""

        job_id = self.job_config['job_id']
        filename = self.job_config['filename']
        expected_format = self.job_config['file_format']

        logger.info(f"Starting file validation for job {job_id}: {filename}")

        try:
            # Update job status
            self._update_job_status('validating', 'Starting file validation')

            # Check file exists
            file_path = f"/app/data/input/{filename}"
            validation_results = self._validate_file(file_path, expected_format)

            if validation_results['valid']:
                logger.info(f"File validation successful for job {job_id}")
                self._update_job_status('validated', 'File validation completed successfully')
                return validation_results
            else:
                error_msg = f"File validation failed: {validation_results['error']}"
                logger.error(error_msg)
                self._update_job_status('failed', error_msg)
                raise ValueError(error_msg)

        except Exception as e:
            error_msg = f"File validation failed: {str(e)}"
            logger.error(error_msg)
            self._update_job_status('failed', error_msg)
            raise

    def _validate_file(self, file_path: str, expected_format: str) -> Dict[str, Any]:
        """Perform comprehensive file validation"""

        validation_results = {
            'valid': False,
            'error': None,
            'file_size': 0,
            'mime_type': None,
            'format_confirmed': False,
            'encoding': None,
            'structure_valid': False
        }

        try:
            # Check if file exists
            if not os.path.exists(file_path):
                validation_results['error'] = f"File not found: {file_path}"
                return validation_results

            # Check file size
            file_size = os.path.getsize(file_path)
            validation_results['file_size'] = file_size

            if file_size == 0:
                validation_results['error'] = "File is empty"
                return validation_results

            # Check file size limit (10GB)
            max_size = 10 * 1024 * 1024 * 1024  # 10GB
            if file_size > max_size:
                validation_results['error'] = f"File too large: {file_size} bytes (max: {max_size} bytes)"
                return validation_results

            # Check MIME type
            try:
                mime_type = magic.from_file(file_path, mime=True)
                validation_results['mime_type'] = mime_type
            except Exception:
                # Fallback if python-magic is not available
                validation_results['mime_type'] = 'unknown'

            # Validate format-specific structure
            if expected_format == 'csv':
                validation_results.update(self._validate_csv_structure(file_path))
            elif expected_format == 'json':
                validation_results.update(self._validate_json_structure(file_path))
            elif expected_format == 'xml':
                validation_results.update(self._validate_xml_structure(file_path))
            else:
                validation_results['error'] = f"Unsupported file format: {expected_format}"
                return validation_results

            # Final validation check
            if validation_results['structure_valid'] and not validation_results['error']:
                validation_results['valid'] = True
                validation_results['format_confirmed'] = True

            return validation_results

        except Exception as e:
            validation_results['error'] = f"Validation error: {str(e)}"
            return validation_results

    def _validate_csv_structure(self, file_path: str) -> Dict[str, Any]:
        """Validate CSV file structure"""

        results = {'structure_valid': False, 'encoding': 'utf-8', 'rows_sampled': 0}

        try:
            import pandas as pd
            import chardet

            # Detect encoding
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # Read first 10KB
                encoding_result = chardet.detect(raw_data)
                results['encoding'] = encoding_result.get('encoding', 'utf-8')

            # Try to read first few rows with pandas
            try:
                df_sample = pd.read_csv(file_path, encoding=results['encoding'], nrows=100)
                results['rows_sampled'] = len(df_sample)

                # Check if we have at least one column
                if len(df_sample.columns) == 0:
                    results['error'] = "CSV file has no columns"
                    return results

                # Check for completely empty dataframe
                if df_sample.empty:
                    results['error'] = "CSV file has no data rows"
                    return results

                results['structure_valid'] = True
                logger.info(f"CSV validation successful: {len(df_sample.columns)} columns, {results['rows_sampled']} rows sampled")

            except pd.errors.EmptyDataError:
                results['error'] = "CSV file appears to be empty or malformed"
            except pd.errors.ParserError as e:
                results['error'] = f"CSV parsing error: {str(e)}"
            except UnicodeDecodeError:
                results['error'] = f"Unable to decode CSV file with encoding: {results['encoding']}"

        except Exception as e:
            results['error'] = f"CSV validation failed: {str(e)}"

        return results

    def _validate_json_structure(self, file_path: str) -> Dict[str, Any]:
        """Validate JSON file structure"""

        results = {'structure_valid': False, 'encoding': 'utf-8', 'json_type': 'unknown'}

        try:
            import json

            # Try to parse JSON
            with open(file_path, 'r', encoding='utf-8') as f:
                # Check if it's JSONL format
                first_line = f.readline().strip()
                f.seek(0)

                if first_line.startswith('['):
                    # JSON array
                    data = json.load(f)
                    if isinstance(data, list):
                        results['json_type'] = 'array'
                        results['records_count'] = len(data)
                    else:
                        results['json_type'] = 'object'
                        results['records_count'] = 1
                else:
                    # Try JSONL format
                    lines = f.readlines()
                    valid_lines = 0
                    for line in lines[:100]:  # Check first 100 lines
                        if line.strip():
                            json.loads(line.strip())  # This will raise exception if invalid
                            valid_lines += 1

                    results['json_type'] = 'jsonl'
                    results['records_count'] = len(lines)

            results['structure_valid'] = True
            logger.info(f"JSON validation successful: {results['json_type']} format, {results.get('records_count', 0)} records")

        except json.JSONDecodeError as e:
            results['error'] = f"Invalid JSON format: {str(e)}"
        except UnicodeDecodeError:
            results['error'] = "Unable to decode JSON file as UTF-8"
        except Exception as e:
            results['error'] = f"JSON validation failed: {str(e)}"

        return results

    def _validate_xml_structure(self, file_path: str) -> Dict[str, Any]:
        """Validate XML file structure"""

        results = {'structure_valid': False, 'encoding': 'utf-8', 'root_element': None}

        try:
            import xml.etree.ElementTree as ET

            # Try to parse XML
            tree = ET.parse(file_path)
            root = tree.getroot()

            results['root_element'] = root.tag
            results['structure_valid'] = True

            # Count child elements
            child_count = len(list(root))
            results['child_elements'] = child_count

            logger.info(f"XML validation successful: root='{root.tag}', {child_count} child elements")

        except ET.ParseError as e:
            results['error'] = f"Invalid XML format: {str(e)}"
        except Exception as e:
            results['error'] = f"XML validation failed: {str(e)}"

        return results

    def _update_job_status(self, status: str, message: str = None):
        """Update job status in database"""

        try:
            pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')

            if message:
                pg_hook.run(
                    """UPDATE processing_jobs
                       SET status = %s, error_message = %s, updated_at = CURRENT_TIMESTAMP
                       WHERE job_id = %s""",
                    parameters=[status, message, self.job_config['job_id']]
                )
            else:
                pg_hook.run(
                    """UPDATE processing_jobs
                       SET status = %s, updated_at = CURRENT_TIMESTAMP
                       WHERE job_id = %s""",
                    parameters=[status, self.job_config['job_id']]
                )

            logger.debug(f"Updated job status to '{status}' for job {self.job_config['job_id']}")

        except Exception as e:
            logger.warning(f"Failed to update job status: {str(e)}")


class FileIntegrityOperator(BaseOperator):
    """
    Operator for checking file integrity (checksums, corruption detection)
    """

    def __init__(self, job_config: Dict[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_config = job_config

    def execute(self, context: Context):
        """Check file integrity"""

        job_id = self.job_config['job_id']
        filename = self.job_config['filename']
        file_path = f"/app/data/input/{filename}"

        logger.info(f"Starting file integrity check for job {job_id}")

        try:
            integrity_results = self._check_file_integrity(file_path)

            if integrity_results['intact']:
                logger.info(f"File integrity check successful for job {job_id}")
                return integrity_results
            else:
                error_msg = f"File integrity check failed: {integrity_results['error']}"
                logger.error(error_msg)
                raise ValueError(error_msg)

        except Exception as e:
            error_msg = f"File integrity check failed: {str(e)}"
            logger.error(error_msg)
            raise

    def _check_file_integrity(self, file_path: str) -> Dict[str, Any]:
        """Perform file integrity checks"""

        results = {
            'intact': False,
            'error': None,
            'md5_hash': None,
            'sha256_hash': None,
            'readable': False
        }

        try:
            # Check if file is readable
            with open(file_path, 'rb') as f:
                # Try to read first and last chunks
                first_chunk = f.read(1024)
                f.seek(-1024, 2)  # Go to near end
                last_chunk = f.read(1024)

                if len(first_chunk) > 0 and len(last_chunk) > 0:
                    results['readable'] = True

            # Calculate file hashes for integrity verification
            import hashlib

            with open(file_path, 'rb') as f:
                md5_hash = hashlib.md5()
                sha256_hash = hashlib.sha256()

                # Read file in chunks to handle large files
                for chunk in iter(lambda: f.read(8192), b""):
                    md5_hash.update(chunk)
                    sha256_hash.update(chunk)

                results['md5_hash'] = md5_hash.hexdigest()
                results['sha256_hash'] = sha256_hash.hexdigest()

            if results['readable']:
                results['intact'] = True

            logger.info(f"File integrity check completed. MD5: {results['md5_hash'][:8]}...")

        except Exception as e:
            results['error'] = str(e)

        return results