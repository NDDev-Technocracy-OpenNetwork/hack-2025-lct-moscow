import dlt
import xml.etree.ElementTree as ET
import xmltodict
import os
from typing import Iterator, Dict, Any, List
from .base_pipeline import BasePipeline


class XMLPipeline(BasePipeline):
    """Pipeline for processing XML files"""

    def __init__(self, job_config: Dict[str, Any]):
        super().__init__(job_config)
        self.chunk_size = job_config.get('chunk_size', 1000)  # Smaller for XML
        self.file_path = f"/app/data/input/{self.filename}"
        self.total_records = self.estimate_total_records(self.file_path)

    def create_pipeline(self) -> dlt.Pipeline:
        """Create dlt pipeline based on destination"""
        return self._create_pipeline_by_destination()

    def create_resource(self):
        """Create dlt resource for XML data"""
        return self._xml_data_resource()

    @dlt.resource(table_name=lambda: dlt.config.get("table_name", "xml_data"))
    def _xml_data_resource(self) -> Iterator[List[Dict]]:
        """DLT resource for reading XML file"""

        try:
            self.update_progress(0, "analyzing", "Analyzing XML file structure")

            # Choose processing method based on file size
            file_size = os.path.getsize(self.file_path)
            if file_size > 100 * 1024 * 1024:  # 100MB
                self.logger.info("Using iterative parsing for large XML file")
                yield from self._read_xml_iterative()
            else:
                self.logger.info("Using full parsing for small XML file")
                yield from self._read_xml_full()

        except Exception as e:
            self.logger.error(f"Error reading XML file: {str(e)}")
            raise

    def _read_xml_iterative(self) -> Iterator[List[Dict]]:
        """Iteratively parse large XML file"""
        batch = []
        record_count = 0

        try:
            # Get iterator for XML parsing
            context = ET.iterparse(self.file_path, events=('start', 'end'))
            context = iter(context)
            event, root = next(context)
            root_tag = root.tag

            for event, elem in context:
                if event == 'end' and elem.tag != root_tag:
                    # Process each child element as a record
                    record = self._xml_element_to_dict(elem)
                    if record:  # Only add non-empty records
                        batch.append(record)
                        record_count += 1

                    if len(batch) >= self.chunk_size:
                        self.update_progress(
                            len(batch),
                            f"processing_batch_{record_count // self.chunk_size}",
                            f"Processed batch with {len(batch)} records"
                        )
                        yield batch
                        batch = []

                    # Clear processed elements to free memory
                    elem.clear()
                    root.clear()

            # Yield final batch
            if batch:
                self.update_progress(
                    len(batch),
                    "processing_final_batch",
                    f"Processed final batch with {len(batch)} records"
                )
                yield batch

            self.logger.info(f"XML iterative processing completed. Total records: {record_count}")

        except ET.ParseError as e:
            raise ValueError(f"Invalid XML format: {e}")

    def _read_xml_full(self) -> Iterator[List[Dict]]:
        """Parse entire XML file into memory"""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                xml_content = f.read()

            # Parse XML to dictionary
            xml_data = xmltodict.parse(xml_content)

            # Extract records from the XML structure
            records = self._extract_records_from_xml(xml_data)
            total_records = len(records)

            self.logger.info(f"Processing XML with {total_records} records")

            # Process records in chunks
            for i in range(0, total_records, self.chunk_size):
                chunk = records[i:i + self.chunk_size]
                processed_chunk = [self._flatten_xml_record(record) for record in chunk]

                self.update_progress(
                    len(processed_chunk),
                    f"processing_chunk_{i // self.chunk_size + 1}",
                    f"Processed chunk with {len(processed_chunk)} records"
                )
                yield processed_chunk

        except ET.ParseError as e:
            raise ValueError(f"Invalid XML format: {e}")

    def _xml_element_to_dict(self, element) -> Dict[str, Any]:
        """Convert XML element to dictionary"""
        result = {}

        # Add attributes with @ prefix
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
        child_counts = {}
        for child in element:
            child_tag = child.tag

            # Count occurrences of each child tag
            if child_tag not in child_counts:
                child_counts[child_tag] = 0
            child_counts[child_tag] += 1

            child_data = self._xml_element_to_dict(child)

            if child_tag in result:
                # Convert to list if multiple children with same tag
                if not isinstance(result[child_tag], list):
                    result[child_tag] = [result[child_tag]]
                result[child_tag].append(child_data)
            else:
                result[child_tag] = child_data

        return result

    def _extract_records_from_xml(self, xml_data: Dict) -> List[Dict]:
        """Extract list of records from XML structure"""

        def find_repeating_elements(data, level=0):
            """Find repeating elements that likely represent records"""
            if level > 5:  # Limit recursion depth
                return []

            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                # Look for the largest array in the structure
                largest_array = []
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > len(largest_array):
                        # Check if array contains objects (not just primitives)
                        if value and isinstance(value[0], dict):
                            largest_array = value
                    elif isinstance(value, dict):
                        nested_result = find_repeating_elements(value, level + 1)
                        if len(nested_result) > len(largest_array):
                            largest_array = nested_result

                return largest_array

            return []

        records = find_repeating_elements(xml_data)

        if not records:
            # If no repeating elements found, treat root as single record
            self.logger.info("No repeating elements found, treating root as single record")
            return [xml_data]

        self.logger.info(f"Found {len(records)} records in XML structure")
        return records

    def _flatten_xml_record(self, record: Dict, parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """Flatten nested XML record"""
        items = []

        for k, v in record.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k

            # Clean up key names
            new_key = new_key.replace('@', 'attr_').replace('#', 'text_')

            if isinstance(v, dict):
                items.extend(self._flatten_xml_record(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                if v and isinstance(v[0], dict):
                    # Array of objects - flatten first item as representative
                    items.extend(self._flatten_xml_record(v[0], f"{new_key}_item", sep=sep).items())
                    # Also add count
                    items.append((f"{new_key}_count", len(v)))
                else:
                    # Array of primitives - join as string
                    items.append((new_key, ', '.join(str(item) for item in v)))
            else:
                items.append((new_key, v))

        return dict(items)

    def estimate_total_records(self, file_path: str) -> int:
        """Estimate total number of records in XML file"""
        if not os.path.exists(file_path):
            return 0

        try:
            file_size = os.path.getsize(file_path)

            # For very small files, try to parse and count
            if file_size < 1024 * 1024:  # 1MB
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    xml_data = xmltodict.parse(content)
                    records = self._extract_records_from_xml(xml_data)
                    actual_count = len(records)
                    self.logger.info(f"Actual record count: {actual_count}")
                    return actual_count
                except:
                    pass

            # For larger files, estimate based on file size
            # Rough estimate: average 200 bytes per XML record
            estimated = file_size // 200
            self.logger.info(f"Estimated {estimated} records in XML file (file size: {file_size} bytes)")
            return estimated

        except Exception as e:
            self.logger.warning(f"Could not estimate record count: {e}")
            return 0