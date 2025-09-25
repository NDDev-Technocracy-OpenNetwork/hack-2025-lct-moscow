# Задача 11: Testing with Big Files

## Цель
Комплексное тестирование системы с большими файлами (до 10GB), проверка производительности, стабильности и корректности обработки данных во всех destinations.

## Технические требования
- Тестирование с файлами различных размеров (1MB - 10GB)
- Проверка всех форматов файлов (CSV, JSON, XML)
- Тестирование всех destinations (PostgreSQL, ClickHouse, HDFS)
- Performance testing и memory profiling
- Stress testing и load testing

## Подготовка тестовых данных

### Скрипт генерации больших тестовых файлов

#### scripts/generate_test_data.py
```python
import csv
import json
import xml.etree.ElementTree as ET
import random
import string
import datetime
from typing import Dict, Any, List
import argparse
import os
from faker import Faker

fake = Faker()

class TestDataGenerator:
    """Генератор тестовых данных различных форматов и размеров"""

    def __init__(self, output_dir: str = "./TESTDATA"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def generate_csv_file(self, filename: str, num_records: int, include_nulls: bool = True):
        """Генерация большого CSV файла"""

        filepath = os.path.join(self.output_dir, f"{filename}.csv")

        fieldnames = [
            'id', 'name', 'email', 'age', 'salary', 'department',
            'hire_date', 'is_active', 'address', 'phone', 'description'
        ]

        print(f"Generating CSV file: {filepath} with {num_records:,} records")

        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(num_records):
                row = {
                    'id': i + 1,
                    'name': fake.name() if not (include_nulls and random.random() < 0.05) else None,
                    'email': fake.email() if not (include_nulls and random.random() < 0.03) else None,
                    'age': random.randint(18, 65) if not (include_nulls and random.random() < 0.02) else None,
                    'salary': round(random.uniform(30000, 150000), 2),
                    'department': random.choice(['IT', 'HR', 'Finance', 'Marketing', 'Sales']),
                    'hire_date': fake.date_between(start_date='-10y', end_date='today').isoformat(),
                    'is_active': random.choice([True, False]),
                    'address': fake.address().replace('\n', ', '),
                    'phone': fake.phone_number(),
                    'description': fake.text(max_nb_chars=200) if random.random() < 0.7 else None
                }

                writer.writerow(row)

                if (i + 1) % 100000 == 0:
                    print(f"  Generated {i + 1:,} records...")

        print(f"CSV file generated: {filepath}")
        return filepath

    def generate_json_file(self, filename: str, num_records: int, format_type: str = 'jsonl'):
        """Генерация большого JSON файла"""

        filepath = os.path.join(self.output_dir, f"{filename}.json")

        print(f"Generating JSON file: {filepath} with {num_records:,} records ({format_type})")

        def generate_record(record_id: int) -> Dict[str, Any]:
            return {
                'id': record_id,
                'user_info': {
                    'name': fake.name(),
                    'email': fake.email(),
                    'age': random.randint(18, 65)
                },
                'employment': {
                    'salary': round(random.uniform(30000, 150000), 2),
                    'department': random.choice(['IT', 'HR', 'Finance', 'Marketing', 'Sales']),
                    'hire_date': fake.date_between(start_date='-10y', end_date='today').isoformat(),
                    'is_active': random.choice([True, False])
                },
                'contact': {
                    'address': {
                        'street': fake.street_address(),
                        'city': fake.city(),
                        'country': fake.country(),
                        'postal_code': fake.postcode()
                    },
                    'phone': fake.phone_number()
                },
                'metadata': {
                    'created_at': fake.date_time_between(start_date='-1y', end_date='now').isoformat(),
                    'tags': [fake.word() for _ in range(random.randint(1, 5))],
                    'score': round(random.uniform(0, 100), 2)
                },
                'description': fake.text(max_nb_chars=300) if random.random() < 0.8 else None
            }

        with open(filepath, 'w', encoding='utf-8') as jsonfile:
            if format_type == 'array':
                # JSON массив
                jsonfile.write('[\n')

                for i in range(num_records):
                    record = generate_record(i + 1)
                    json.dump(record, jsonfile, ensure_ascii=False)

                    if i < num_records - 1:
                        jsonfile.write(',\n')
                    else:
                        jsonfile.write('\n')

                    if (i + 1) % 50000 == 0:
                        print(f"  Generated {i + 1:,} records...")

                jsonfile.write(']')

            else:  # JSONL format
                for i in range(num_records):
                    record = generate_record(i + 1)
                    json.dump(record, jsonfile, ensure_ascii=False)
                    jsonfile.write('\n')

                    if (i + 1) % 50000 == 0:
                        print(f"  Generated {i + 1:,} records...")

        print(f"JSON file generated: {filepath}")
        return filepath

    def generate_xml_file(self, filename: str, num_records: int):
        """Генерация большого XML файла"""

        filepath = os.path.join(self.output_dir, f"{filename}.xml")

        print(f"Generating XML file: {filepath} with {num_records:,} records")

        # Создаем корневой элемент
        root = ET.Element("employees")

        for i in range(num_records):
            employee = ET.SubElement(root, "employee")
            employee.set("id", str(i + 1))

            # Основная информация
            ET.SubElement(employee, "name").text = fake.name()
            ET.SubElement(employee, "email").text = fake.email()
            ET.SubElement(employee, "age").text = str(random.randint(18, 65))

            # Информация о работе
            employment = ET.SubElement(employee, "employment")
            ET.SubElement(employment, "salary").text = str(round(random.uniform(30000, 150000), 2))
            ET.SubElement(employment, "department").text = random.choice(['IT', 'HR', 'Finance', 'Marketing', 'Sales'])
            ET.SubElement(employment, "hire_date").text = fake.date_between(start_date='-10y', end_date='today').isoformat()
            ET.SubElement(employment, "is_active").text = str(random.choice([True, False])).lower()

            # Контактная информация
            contact = ET.SubElement(employee, "contact")
            address = ET.SubElement(contact, "address")
            ET.SubElement(address, "street").text = fake.street_address()
            ET.SubElement(address, "city").text = fake.city()
            ET.SubElement(address, "country").text = fake.country()
            ET.SubElement(address, "postal_code").text = fake.postcode()

            ET.SubElement(contact, "phone").text = fake.phone_number()

            # Дополнительная информация
            if random.random() < 0.8:
                ET.SubElement(employee, "description").text = fake.text(max_nb_chars=200)

            if (i + 1) % 10000 == 0:
                print(f"  Generated {i + 1:,} records...")

        # Запись XML в файл
        tree = ET.ElementTree(root)
        ET.indent(tree, space="  ", level=0)  # Форматирование для читаемости
        tree.write(filepath, encoding='utf-8', xml_declaration=True)

        print(f"XML file generated: {filepath}")
        return filepath

    def generate_test_suite(self):
        """Генерация полного набора тестовых файлов"""

        test_files = []

        # Малые файлы для быстрого тестирования
        test_files.append(self.generate_csv_file("small_csv_1k", 1000))
        test_files.append(self.generate_json_file("small_json_1k", 1000, 'jsonl'))
        test_files.append(self.generate_xml_file("small_xml_1k", 1000))

        # Средние файлы
        test_files.append(self.generate_csv_file("medium_csv_100k", 100000))
        test_files.append(self.generate_json_file("medium_json_100k", 100000, 'array'))
        test_files.append(self.generate_xml_file("medium_xml_50k", 50000))

        # Большие файлы
        test_files.append(self.generate_csv_file("large_csv_1m", 1000000))
        test_files.append(self.generate_json_file("large_json_1m", 1000000, 'jsonl'))
        test_files.append(self.generate_xml_file("large_xml_500k", 500000))

        # Очень большие файлы (для стресс-тестирования)
        test_files.append(self.generate_csv_file("xlarge_csv_10m", 10000000))
        test_files.append(self.generate_json_file("xlarge_json_5m", 5000000, 'jsonl'))

        print(f"\nGenerated {len(test_files)} test files:")
        for filepath in test_files:
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            print(f"  {os.path.basename(filepath)}: {size_mb:.1f} MB")

        return test_files

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate test data files')
    parser.add_argument('--output-dir', default='./TESTDATA', help='Output directory for test files')
    parser.add_argument('--file-type', choices=['csv', 'json', 'xml', 'all'], default='all', help='Type of file to generate')
    parser.add_argument('--records', type=int, default=1000000, help='Number of records to generate')
    parser.add_argument('--filename', default='test_data', help='Base filename')

    args = parser.parse_args()

    generator = TestDataGenerator(args.output_dir)

    if args.file_type == 'all':
        generator.generate_test_suite()
    elif args.file_type == 'csv':
        generator.generate_csv_file(args.filename, args.records)
    elif args.file_type == 'json':
        generator.generate_json_file(args.filename, args.records)
    elif args.file_type == 'xml':
        generator.generate_xml_file(args.filename, args.records)
```

## Тестовые сценарии

### tests/integration/test_big_files.py
```python
import pytest
import asyncio
import time
import psutil
import os
from typing import Dict, Any, List
import logging

from app.services.api import api_client
from app.core.websocket_manager import WebSocketManager
from tests.utils.test_helpers import create_test_job, wait_for_job_completion

logger = logging.getLogger(__name__)

class TestBigFiles:
    """Интеграционные тесты для больших файлов"""

    @pytest.fixture(autouse=True)
    def setup_test_environment(self):
        """Настройка тестового окружения"""

        # Мониторинг системных ресурсов
        self.initial_memory = psutil.virtual_memory().used
        self.initial_cpu = psutil.cpu_percent()

        yield

        # Проверка утечек памяти после теста
        final_memory = psutil.virtual_memory().used
        memory_diff = final_memory - self.initial_memory

        # Предупреждение о значительном росте памяти
        if memory_diff > 500 * 1024 * 1024:  # 500MB
            logger.warning(f"Memory usage increased by {memory_diff / 1024 / 1024:.1f} MB during test")

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_large_csv_processing(self):
        """Тест обработки большого CSV файла (1M записей)"""

        test_config = {
            'filename': 'large_csv_1m.csv',
            'destination_type': 'postgresql',
            'destination_config': {
                'host': 'postgres',
                'port': 5432,
                'database': 'test_db',
                'schema': 'public',
                'table_prefix': 'test_'
            }
        }

        # Мониторинг производительности
        start_time = time.time()
        start_memory = psutil.virtual_memory().used

        # Запуск обработки файла
        job = await create_test_job(test_config)

        # Ожидание завершения с таймаутом
        result = await wait_for_job_completion(
            job['job_id'],
            timeout_minutes=30,
            check_interval=5
        )

        end_time = time.time()
        end_memory = psutil.virtual_memory().used

        # Проверка результатов
        assert result['status'] == 'completed'
        assert result['records_processed'] > 900000  # Ожидаем обработку большинства записей

        # Проверка производительности
        processing_time = end_time - start_time
        records_per_second = result['records_processed'] / processing_time
        memory_used = (end_memory - start_memory) / 1024 / 1024  # MB

        logger.info(f"CSV Processing Performance:")
        logger.info(f"  Records processed: {result['records_processed']:,}")
        logger.info(f"  Processing time: {processing_time:.2f} seconds")
        logger.info(f"  Records per second: {records_per_second:.2f}")
        logger.info(f"  Memory used: {memory_used:.1f} MB")

        # Проверка разумных лимитов производительности
        assert records_per_second > 1000, f"Too slow processing: {records_per_second:.2f} rec/sec"
        assert memory_used < 2048, f"Too much memory used: {memory_used:.1f} MB"

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_large_json_processing(self):
        """Тест обработки большого JSON файла"""

        test_config = {
            'filename': 'large_json_1m.json',
            'destination_type': 'clickhouse',
            'destination_config': {
                'host': 'clickhouse',
                'port': 8123,
                'database': 'test_analytics',
                'table_prefix': 'json_test_'
            }
        }

        start_time = time.time()

        job = await create_test_job(test_config)
        result = await wait_for_job_completion(job['job_id'], timeout_minutes=45)

        processing_time = time.time() - start_time

        assert result['status'] == 'completed'
        assert result['records_processed'] > 900000

        # JSON обычно медленнее CSV из-за парсинга
        records_per_second = result['records_processed'] / processing_time
        logger.info(f"JSON Processing: {records_per_second:.2f} rec/sec")

        assert records_per_second > 500, "JSON processing too slow"

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_xml_processing(self):
        """Тест обработки XML файла"""

        test_config = {
            'filename': 'large_xml_500k.xml',
            'destination_type': 'hdfs',
            'destination_config': {
                'namenode_url': 'hdfs://namenode:9000',
                'base_path': '/test_data',
                'file_format': 'parquet',
                'compression': 'snappy'
            }
        }

        start_time = time.time()

        job = await create_test_job(test_config)
        result = await wait_for_job_completion(job['job_id'], timeout_minutes=60)

        processing_time = time.time() - start_time

        assert result['status'] == 'completed'
        assert result['records_processed'] > 450000

        # XML самый медленный для парсинга
        records_per_second = result['records_processed'] / processing_time
        logger.info(f"XML Processing: {records_per_second:.2f} rec/sec")

        assert records_per_second > 100, "XML processing too slow"

    @pytest.mark.asyncio
    @pytest.mark.stress
    async def test_concurrent_processing(self):
        """Стресс-тест: несколько файлов одновременно"""

        test_configs = [
            {
                'filename': 'medium_csv_100k.csv',
                'destination_type': 'postgresql',
                'destination_config': {
                    'host': 'postgres',
                    'port': 5432,
                    'database': 'test_db',
                    'schema': 'concurrent_test',
                    'table_prefix': 'csv_'
                }
            },
            {
                'filename': 'medium_json_100k.json',
                'destination_type': 'clickhouse',
                'destination_config': {
                    'host': 'clickhouse',
                    'port': 8123,
                    'database': 'concurrent_test',
                    'table_prefix': 'json_'
                }
            },
            {
                'filename': 'small_xml_1k.xml',
                'destination_type': 'hdfs',
                'destination_config': {
                    'namenode_url': 'hdfs://namenode:9000',
                    'base_path': '/concurrent_test',
                    'file_format': 'parquet'
                }
            }
        ]

        start_time = time.time()

        # Запуск всех задач одновременно
        jobs = []
        for config in test_configs:
            job = await create_test_job(config)
            jobs.append(job)

        # Ожидание завершения всех задач
        results = await asyncio.gather(*[
            wait_for_job_completion(job['job_id'], timeout_minutes=20)
            for job in jobs
        ])

        total_time = time.time() - start_time

        # Проверка всех результатов
        for i, result in enumerate(results):
            assert result['status'] == 'completed', f"Job {i} failed: {result.get('error_message')}"
            logger.info(f"Job {i} processed {result['records_processed']:,} records")

        total_records = sum(r['records_processed'] for r in results)
        overall_throughput = total_records / total_time

        logger.info(f"Concurrent processing:")
        logger.info(f"  Total records: {total_records:,}")
        logger.info(f"  Total time: {total_time:.2f} seconds")
        logger.info(f"  Overall throughput: {overall_throughput:.2f} rec/sec")

        # Система должна справляться с concurrent обработкой
        assert overall_throughput > 2000, "Concurrent processing too slow"

    @pytest.mark.asyncio
    @pytest.mark.extreme
    async def test_extreme_large_file(self):
        """Экстремальный тест: очень большой файл (10M записей)"""

        test_config = {
            'filename': 'xlarge_csv_10m.csv',
            'destination_type': 'postgresql',
            'destination_config': {
                'host': 'postgres',
                'port': 5432,
                'database': 'test_db',
                'schema': 'extreme_test',
                'table_prefix': 'xlarge_'
            }
        }

        # Мониторинг системных ресурсов во время обработки
        start_time = time.time()
        start_memory = psutil.virtual_memory().used

        job = await create_test_job(test_config)

        # Мониторинг прогресса каждые 30 секунд
        result = await wait_for_job_completion(
            job['job_id'],
            timeout_minutes=120,  # 2 часа максимум
            check_interval=30,
            progress_callback=lambda progress: logger.info(
                f"Progress: {progress.get('records_processed', 0):,} / "
                f"{progress.get('records_total', 0):,} records "
                f"({progress.get('percentage', 0):.1f}%)"
            )
        )

        total_time = time.time() - start_time
        final_memory = psutil.virtual_memory().used
        memory_used = (final_memory - start_memory) / 1024 / 1024  # MB

        assert result['status'] == 'completed'
        assert result['records_processed'] > 9500000  # Ожидаем 95%+ записей

        records_per_second = result['records_processed'] / total_time

        logger.info(f"Extreme file processing:")
        logger.info(f"  Records processed: {result['records_processed']:,}")
        logger.info(f"  Total time: {total_time / 60:.2f} minutes")
        logger.info(f"  Records per second: {records_per_second:.2f}")
        logger.info(f"  Memory used: {memory_used:.1f} MB")

        # Проверка производительности для экстремально больших файлов
        assert records_per_second > 500, f"Extreme file processing too slow: {records_per_second}"
        assert memory_used < 4096, f"Too much memory for extreme file: {memory_used:.1f} MB"

    @pytest.mark.asyncio
    async def test_memory_efficiency(self):
        """Тест эффективности использования памяти"""

        test_config = {
            'filename': 'large_csv_1m.csv',
            'destination_type': 'postgresql',
            'destination_config': {
                'host': 'postgres',
                'port': 5432,
                'database': 'test_db',
                'schema': 'memory_test',
                'table_prefix': 'mem_'
            }
        }

        # Мониторинг памяти в процессе обработки
        memory_samples = []

        async def memory_monitor():
            while True:
                memory_info = psutil.virtual_memory()
                memory_samples.append({
                    'timestamp': time.time(),
                    'used_mb': memory_info.used / 1024 / 1024,
                    'percent': memory_info.percent
                })
                await asyncio.sleep(5)

        # Запуск мониторинга памяти
        monitor_task = asyncio.create_task(memory_monitor())

        try:
            job = await create_test_job(test_config)
            result = await wait_for_job_completion(job['job_id'], timeout_minutes=30)

            assert result['status'] == 'completed'

        finally:
            monitor_task.cancel()

        # Анализ использования памяти
        if memory_samples:
            max_memory = max(s['used_mb'] for s in memory_samples)
            min_memory = min(s['used_mb'] for s in memory_samples)
            avg_memory = sum(s['used_mb'] for s in memory_samples) / len(memory_samples)

            logger.info(f"Memory usage during processing:")
            logger.info(f"  Min: {min_memory:.1f} MB")
            logger.info(f"  Max: {max_memory:.1f} MB")
            logger.info(f"  Avg: {avg_memory:.1f} MB")
            logger.info(f"  Peak difference: {max_memory - min_memory:.1f} MB")

            # Проверка, что память не растет чрезмерно
            memory_growth = max_memory - min_memory
            assert memory_growth < 1024, f"Excessive memory growth: {memory_growth:.1f} MB"

    @pytest.mark.asyncio
    async def test_error_recovery(self):
        """Тест восстановления после ошибок"""

        # Тест с некорректным файлом
        test_config = {
            'filename': 'nonexistent_file.csv',
            'destination_type': 'postgresql',
            'destination_config': {
                'host': 'postgres',
                'port': 5432,
                'database': 'test_db',
                'schema': 'error_test',
                'table_prefix': 'err_'
            }
        }

        job = await create_test_job(test_config)
        result = await wait_for_job_completion(job['job_id'], timeout_minutes=5)

        # Проверка корректной обработки ошибки
        assert result['status'] == 'failed'
        assert 'not found' in result.get('error_message', '').lower()

        # Проверка, что система осталась стабильной после ошибки
        health_check = await api_client.get('/health')
        assert health_check.status_code == 200
```

## Performance Benchmarks

### tests/performance/benchmark_suite.py
```python
import time
import asyncio
import statistics
import json
import os
from typing import Dict, List, Any
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class BenchmarkResult:
    """Результат benchmark теста"""
    test_name: str
    file_size_mb: float
    records_count: int
    processing_time_seconds: float
    records_per_second: float
    memory_used_mb: float
    cpu_usage_percent: float
    success: bool
    error_message: str = None

class PerformanceBenchmark:
    """Benchmark тесты производительности"""

    def __init__(self):
        self.results: List[BenchmarkResult] = []

    async def run_full_benchmark_suite(self) -> List[BenchmarkResult]:
        """Полный набор benchmark тестов"""

        benchmark_configs = [
            # CSV файлы различных размеров
            {'filename': 'small_csv_1k.csv', 'format': 'csv', 'size_category': 'small'},
            {'filename': 'medium_csv_100k.csv', 'format': 'csv', 'size_category': 'medium'},
            {'filename': 'large_csv_1m.csv', 'format': 'csv', 'size_category': 'large'},
            {'filename': 'xlarge_csv_10m.csv', 'format': 'csv', 'size_category': 'xlarge'},

            # JSON файлы
            {'filename': 'small_json_1k.json', 'format': 'json', 'size_category': 'small'},
            {'filename': 'medium_json_100k.json', 'format': 'json', 'size_category': 'medium'},
            {'filename': 'large_json_1m.json', 'format': 'json', 'size_category': 'large'},

            # XML файлы
            {'filename': 'small_xml_1k.xml', 'format': 'xml', 'size_category': 'small'},
            {'filename': 'medium_xml_50k.xml', 'format': 'xml', 'size_category': 'medium'},
            {'filename': 'large_xml_500k.xml', 'format': 'xml', 'size_category': 'large'},
        ]

        destinations = ['postgresql', 'clickhouse', 'hdfs']

        for config in benchmark_configs:
            for destination in destinations:
                result = await self._run_single_benchmark(config, destination)
                self.results.append(result)

                # Логирование результата
                if result.success:
                    logger.info(f"✅ {result.test_name}: {result.records_per_second:.2f} rec/sec")
                else:
                    logger.error(f"❌ {result.test_name}: {result.error_message}")

        return self.results

    async def _run_single_benchmark(self, file_config: Dict, destination: str) -> BenchmarkResult:
        """Выполнение одного benchmark теста"""

        test_name = f"{file_config['filename']}_{destination}"

        try:
            # Конфигурация теста
            test_config = {
                'filename': file_config['filename'],
                'destination_type': destination,
                'destination_config': self._get_destination_config(destination)
            }

            # Получение размера файла
            file_path = os.path.join('./TESTDATA', file_config['filename'])
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024) if os.path.exists(file_path) else 0

            # Запуск обработки с мониторингом
            start_time = time.time()
            start_memory = psutil.virtual_memory().used

            job = await create_test_job(test_config)
            result = await wait_for_job_completion(job['job_id'], timeout_minutes=60)

            end_time = time.time()
            end_memory = psutil.virtual_memory().used

            # Расчет метрик
            processing_time = end_time - start_time
            records_per_second = result['records_processed'] / processing_time if processing_time > 0 else 0
            memory_used_mb = (end_memory - start_memory) / (1024 * 1024)

            return BenchmarkResult(
                test_name=test_name,
                file_size_mb=file_size_mb,
                records_count=result['records_processed'],
                processing_time_seconds=processing_time,
                records_per_second=records_per_second,
                memory_used_mb=memory_used_mb,
                cpu_usage_percent=0,  # TODO: add CPU monitoring
                success=result['status'] == 'completed'
            )

        except Exception as e:
            logger.error(f"Benchmark failed for {test_name}: {e}")

            return BenchmarkResult(
                test_name=test_name,
                file_size_mb=0,
                records_count=0,
                processing_time_seconds=0,
                records_per_second=0,
                memory_used_mb=0,
                cpu_usage_percent=0,
                success=False,
                error_message=str(e)
            )

    def _get_destination_config(self, destination: str) -> Dict[str, Any]:
        """Получение конфигурации для destination"""

        configs = {
            'postgresql': {
                'host': 'postgres',
                'port': 5432,
                'database': 'benchmark_db',
                'schema': 'benchmark',
                'table_prefix': 'bench_'
            },
            'clickhouse': {
                'host': 'clickhouse',
                'port': 8123,
                'database': 'benchmark',
                'table_prefix': 'bench_'
            },
            'hdfs': {
                'namenode_url': 'hdfs://namenode:9000',
                'base_path': '/benchmark',
                'file_format': 'parquet',
                'compression': 'snappy'
            }
        }

        return configs.get(destination, {})

    def generate_performance_report(self) -> Dict[str, Any]:
        """Генерация отчета о производительности"""

        if not self.results:
            return {'error': 'No benchmark results available'}

        # Группировка результатов по различным критериям
        by_format = {}
        by_destination = {}
        by_size = {}

        successful_results = [r for r in self.results if r.success]

        for result in successful_results:
            # По формату файла
            format_name = result.test_name.split('_')[1]  # csv, json, xml
            if format_name not in by_format:
                by_format[format_name] = []
            by_format[format_name].append(result)

            # По destination
            destination = result.test_name.split('_')[-1]
            if destination not in by_destination:
                by_destination[destination] = []
            by_destination[destination].append(result)

            # По размеру файла
            size_category = self._get_size_category(result.file_size_mb)
            if size_category not in by_size:
                by_size[size_category] = []
            by_size[size_category].append(result)

        # Расчет статистики
        report = {
            'summary': {
                'total_tests': len(self.results),
                'successful_tests': len(successful_results),
                'failed_tests': len(self.results) - len(successful_results),
                'success_rate': len(successful_results) / len(self.results) * 100 if self.results else 0
            },
            'performance_by_format': {},
            'performance_by_destination': {},
            'performance_by_size': {},
            'top_performers': self._get_top_performers(successful_results),
            'recommendations': self._generate_recommendations(successful_results)
        }

        # Статистика по форматам
        for format_name, results in by_format.items():
            throughputs = [r.records_per_second for r in results]
            report['performance_by_format'][format_name] = {
                'avg_throughput': statistics.mean(throughputs),
                'max_throughput': max(throughputs),
                'min_throughput': min(throughputs),
                'median_throughput': statistics.median(throughputs),
                'test_count': len(results)
            }

        # Статистика по destinations
        for dest_name, results in by_destination.items():
            throughputs = [r.records_per_second for r in results]
            report['performance_by_destination'][dest_name] = {
                'avg_throughput': statistics.mean(throughputs),
                'max_throughput': max(throughputs),
                'min_throughput': min(throughputs),
                'median_throughput': statistics.median(throughputs),
                'test_count': len(results)
            }

        # Статистика по размерам
        for size_name, results in by_size.items():
            throughputs = [r.records_per_second for r in results]
            report['performance_by_size'][size_name] = {
                'avg_throughput': statistics.mean(throughputs),
                'max_throughput': max(throughputs),
                'min_throughput': min(throughputs),
                'median_throughput': statistics.median(throughputs),
                'test_count': len(results)
            }

        return report

    def _get_size_category(self, size_mb: float) -> str:
        """Определение категории размера файла"""
        if size_mb < 10:
            return 'small'
        elif size_mb < 100:
            return 'medium'
        elif size_mb < 1000:
            return 'large'
        else:
            return 'xlarge'

    def _get_top_performers(self, results: List[BenchmarkResult]) -> List[Dict[str, Any]]:
        """Получение топ исполнителей"""

        sorted_results = sorted(results, key=lambda r: r.records_per_second, reverse=True)

        return [
            {
                'test_name': r.test_name,
                'throughput': r.records_per_second,
                'file_size_mb': r.file_size_mb,
                'records_count': r.records_count
            }
            for r in sorted_results[:10]
        ]

    def _generate_recommendations(self, results: List[BenchmarkResult]) -> List[str]:
        """Генерация рекомендаций на основе результатов"""

        recommendations = []

        # Анализ по форматам
        by_format = {}
        for result in results:
            format_name = result.test_name.split('_')[1]
            if format_name not in by_format:
                by_format[format_name] = []
            by_format[format_name].append(result.records_per_second)

        format_performance = {
            fmt: statistics.mean(throughputs)
            for fmt, throughputs in by_format.items()
        }

        best_format = max(format_performance, key=format_performance.get)
        worst_format = min(format_performance, key=format_performance.get)

        recommendations.append(f"Best performing format: {best_format.upper()} ({format_performance[best_format]:.0f} rec/sec avg)")
        recommendations.append(f"Worst performing format: {worst_format.upper()} ({format_performance[worst_format]:.0f} rec/sec avg)")

        # Анализ по destinations
        by_dest = {}
        for result in results:
            dest = result.test_name.split('_')[-1]
            if dest not in by_dest:
                by_dest[dest] = []
            by_dest[dest].append(result.records_per_second)

        dest_performance = {
            dest: statistics.mean(throughputs)
            for dest, throughputs in by_dest.items()
        }

        best_dest = max(dest_performance, key=dest_performance.get)
        recommendations.append(f"Best performing destination: {best_dest.upper()} ({dest_performance[best_dest]:.0f} rec/sec avg)")

        return recommendations

    def save_results_to_file(self, filename: str = 'benchmark_results.json'):
        """Сохранение результатов в файл"""

        results_data = {
            'timestamp': time.time(),
            'results': [
                {
                    'test_name': r.test_name,
                    'file_size_mb': r.file_size_mb,
                    'records_count': r.records_count,
                    'processing_time_seconds': r.processing_time_seconds,
                    'records_per_second': r.records_per_second,
                    'memory_used_mb': r.memory_used_mb,
                    'success': r.success,
                    'error_message': r.error_message
                }
                for r in self.results
            ],
            'report': self.generate_performance_report()
        }

        with open(filename, 'w') as f:
            json.dump(results_data, f, indent=2)

        logger.info(f"Benchmark results saved to {filename}")

# Запуск benchmark
async def run_benchmarks():
    benchmark = PerformanceBenchmark()
    results = await benchmark.run_full_benchmark_suite()

    report = benchmark.generate_performance_report()
    benchmark.save_results_to_file()

    print("\n" + "="*50)
    print("PERFORMANCE BENCHMARK REPORT")
    print("="*50)

    print(f"\nSummary:")
    print(f"  Total tests: {report['summary']['total_tests']}")
    print(f"  Successful: {report['summary']['successful_tests']}")
    print(f"  Success rate: {report['summary']['success_rate']:.1f}%")

    print(f"\nTop Performers:")
    for i, performer in enumerate(report['top_performers'][:5], 1):
        print(f"  {i}. {performer['test_name']}: {performer['throughput']:.0f} rec/sec")

    print(f"\nRecommendations:")
    for rec in report['recommendations']:
        print(f"  • {rec}")

if __name__ == "__main__":
    asyncio.run(run_benchmarks())
```

## Задачи для выполнения

### 1. Подготовка тестовых данных
- [ ] Создать генератор больших тестовых файлов
- [ ] Сгенерировать файлы различных размеров (1MB - 10GB)
- [ ] Создать файлы всех поддерживаемых форматов
- [ ] Добавить файлы с различными характеристиками данных

### 2. Интеграционные тесты
- [ ] Тесты обработки больших CSV файлов
- [ ] Тесты обработки больших JSON файлов
- [ ] Тесты обработки XML файлов
- [ ] Стресс-тесты с concurrent обработкой

### 3. Performance тестирование
- [ ] Benchmark suite для всех форматов и destinations
- [ ] Мониторинг системных ресурсов (CPU, память)
- [ ] Анализ throughput и latency
- [ ] Генерация отчетов о производительности

### 4. Мониторинг ресурсов
- [ ] Мониторинг использования памяти
- [ ] Отслеживание CPU usage
- [ ] Проверка дискового пространства
- [ ] Мониторинг сетевого трафика

### 5. Error handling тесты
- [ ] Тесты с некорректными файлами
- [ ] Тесты с недоступными destinations
- [ ] Тесты восстановления после сбоев
- [ ] Проверка stability после ошибок

### 6. Экстремальные тесты
- [ ] Файлы размером до 10GB
- [ ] Очень большое количество файлов одновременно
- [ ] Длительные процессы (несколько часов)
- [ ] Тесты на ограничения системы

### 7. Автоматизация и CI
- [ ] Интеграция тестов в CI/CD pipeline
- [ ] Автоматическое генерирование отчетов
- [ ] Мониторинг регрессии производительности
- [ ] Alert система при деградации

## Ожидаемый результат
После выполнения этой задачи должна быть готова комплексная система тестирования больших файлов с автоматической генерацией данных, performance benchmarks и детальными отчетами о производительности.

## Следующая задача
[12_deployment_optimization.md](./12_deployment_optimization.md) - Оптимизация и деплойменты