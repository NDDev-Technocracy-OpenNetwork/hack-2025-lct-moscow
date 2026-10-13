import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://fixture:fixture@127.0.0.1:65433/fixture')
os.environ.setdefault('AIRFLOW_PASSWORD', 'fixture')
os.environ.setdefault('CLICKHOUSE_PASSWORD', 'fixture')

from app.core.file_processor import FileProcessor
from app.main import app


class PublicationSmoke(unittest.TestCase):
    def test_health_schema_is_registered(self):
        self.assertIn('/health', app.openapi()['paths'])

    def test_csv_json_xml_columns(self):
        async def check():
            with tempfile.TemporaryDirectory() as directory:
                samples = {
                    'csv': 'name,value\na,1\nb,2\n',
                    'json': json.dumps([{'name': 'a', 'value': 1}, {'name': 'b', 'value': 2}]),
                    'xml': '<records><record><name>a</name><value>1</value></record><record><name>b</name><value>2</value></record></records>',
                }
                for suffix, text in samples.items():
                    file = Path(directory) / ('sample.' + suffix)
                    file.write_text(text)
                    result = await FileProcessor().analyze_file(str(file), suffix)
                    self.assertEqual(len(result['columns']), 2)
        asyncio.run(check())
