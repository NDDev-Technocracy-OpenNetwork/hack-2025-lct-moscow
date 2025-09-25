# Задача 5: DLT Integration for ETL Processes

## Цель
Интеграция dlt для выполнения ETL процессов с поддержкой всех целевых хранилищ (PostgreSQL, ClickHouse, HDFS) и обработки больших файлов по частям.

## Технические требования
- Использование dlt для извлечения, трансформации и загрузки данных
- Поддержка streaming обработки для файлов до 10GB
- Конфигурация для всех типов destinations
- Автоматическое создание схем и таблиц
- Обработка ошибок и логирование прогресса

## Структура dlt Worker

```
services/dlt-worker/
├── Dockerfile
├── requirements.txt
├── app/
│   ├── main.py
│   ├── config.py
│   ├── pipelines/
│   │   ├── __init__.py
│   │   ├── csv_pipeline.py
│   │   ├── json_pipeline.py
│   │   ├── xml_pipeline.py
│   │   └── base_pipeline.py
│   ├── destinations/
│   │   ├── __init__.py
│   │   ├── postgresql_dest.py
│   │   ├── clickhouse_dest.py
│   │   └── hdfs_dest.py
│   ├── transformers/
│   │   ├── __init__.py
│   │   ├── data_cleaner.py
│   │   ├── schema_mapper.py
│   │   └── type_converter.py
│   └── utils/
│       ├── __init__.py
│       ├── file_reader.py
│       ├── progress_tracker.py
│       └── error_handler.py
```

## Dockerfile для dlt-worker

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Системные зависимости для различных форматов данных
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    g++ \
    libpq-dev \
    libxml2-dev \
    libxslt-dev \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Java для HDFS клиента
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Python зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование приложения
COPY app/ ./app/

# Создание директорий
RUN mkdir -p /app/data/input /app/data/temp /app/logs

CMD ["python", "app/main.py"]
```

## requirements.txt для dlt-worker

```txt
dlt[postgresql,clickhouse,filesystem,s3]==1.17.0
pandas==2.1.3
pyarrow==14.0.1
fastparquet==2023.10.1
psycopg2-binary==2.9.7
clickhouse-driver==0.2.7
hdfs3==0.3.1
lxml==4.9.3
xmltodict==0.13.0
sqlalchemy==2.0.23
pydantic==2.5.0
python-json-logger==2.0.7
tqdm==4.66.1
```

## Базовый pipeline класс

### base_pipeline.py
```python
import dlt
from abc import ABC, abstractmethod
from typing import Any, Dict, Generator, Optional
import logging
from datetime import datetime
import uuid

class BasePipeline(ABC):
    """Базовый класс для всех ETL pipelines"""

    def __init__(self, job_config: Dict[str, Any]):
        self.job_config = job_config
        self.job_id = job_config['job_id']
        self.filename = job_config['filename']
        self.file_format = job_config['file_format']
        self.destination_type = job_config['destination_type']
        self.destination_config = job_config['destination_config']

        self.logger = logging.getLogger(f"pipeline.{self.job_id}")
        self.progress_callback = None
        self.records_processed = 0

    def set_progress_callback(self, callback):
        """Установка callback для отслеживания прогресса"""
        self.progress_callback = callback

    def update_progress(self, records_count: int, stage: str = "processing"):
        """Обновление прогресса обработки"""
        self.records_processed += records_count

        if self.progress_callback:
            self.progress_callback({
                'job_id': self.job_id,
                'stage': stage,
                'records_processed': self.records_processed,
                'timestamp': datetime.utcnow().isoformat()
            })

    @abstractmethod
    def create_pipeline(self) -> dlt.Pipeline:
        """Создание dlt pipeline с нужной конфигурацией"""
        pass

    @abstractmethod
    def create_resource(self) -> Any:
        """Создание dlt resource для чтения данных"""
        pass

    def run(self) -> Dict[str, Any]:
        """Основной метод выполнения ETL процесса"""
        try:
            self.logger.info(f"Starting ETL process for job {self.job_id}")

            # Создание pipeline
            pipeline = self.create_pipeline()

            # Создание resource
            resource = self.create_resource()

            # Выполнение ETL
            load_info = pipeline.run(resource)

            # Подсчет итоговых метрик
            total_records = sum(
                package.load_info.dataset.get(table, {}).get('rowCount', 0)
                for package in load_info.load_packages
                for table in package.load_info.dataset
            )

            self.logger.info(f"ETL process completed. Processed {total_records} records")

            return {
                'job_id': self.job_id,
                'status': 'completed',
                'records_processed': total_records,
                'load_info': load_info.asdict() if hasattr(load_info, 'asdict') else str(load_info)
            }

        except Exception as e:
            self.logger.error(f"ETL process failed: {str(e)}", exc_info=True)
            raise
```

## CSV Pipeline

### csv_pipeline.py
```python
import dlt
import pandas as pd
from typing import Generator, Dict, Any
from .base_pipeline import BasePipeline
import os

class CSVPipeline(BasePipeline):
    """Pipeline для обработки CSV файлов"""

    def __init__(self, job_config: Dict[str, Any]):
        super().__init__(job_config)
        self.chunk_size = job_config.get('chunk_size', 10000)
        self.file_path = f"/app/data/input/{self.filename}"

    def create_pipeline(self) -> dlt.Pipeline:
        """Создание dlt pipeline в зависимости от destination"""

        pipeline_name = f"csv_job_{self.job_id}"

        if self.destination_type == 'postgresql':
            return dlt.pipeline(
                pipeline_name=pipeline_name,
                destination='postgres',
                dataset_name=self.destination_config.get('schema', 'public'),
                credentials={
                    'host': self.destination_config.get('host', 'postgres'),
                    'port': self.destination_config.get('port', 5432),
                    'database': self.destination_config.get('database', 'user_data'),
                    'username': 'bigdata_user',
                    'password': 'bigdata_password'
                }
            )

        elif self.destination_type == 'clickhouse':
            return dlt.pipeline(
                pipeline_name=pipeline_name,
                destination='clickhouse',
                dataset_name=self.destination_config.get('database', 'analytics'),
                credentials={
                    'host': self.destination_config.get('host', 'clickhouse'),
                    'port': self.destination_config.get('port', 8123),
                    'username': 'bigdata_user',
                    'password': 'bigdata_password'
                }
            )

        elif self.destination_type == 'hdfs':
            return dlt.pipeline(
                pipeline_name=pipeline_name,
                destination='filesystem',
                credentials={
                    'aws_access_key_id': '',  # Для HDFS не нужно
                    'aws_secret_access_key': '',
                },
                dataset_name='hdfs_data'
            )

        else:
            raise ValueError(f"Unsupported destination type: {self.destination_type}")

    @dlt.resource(table_name="csv_data")
    def create_resource(self) -> Generator[pd.DataFrame, None, None]:
        """Чтение CSV файла по частям"""

        try:
            # Определение разделителя и кодировки
            encoding = self._detect_encoding()
            separator = self._detect_separator()

            self.logger.info(f"Reading CSV with encoding={encoding}, separator={separator}")

            # Чтение файла по частям
            chunk_count = 0
            for chunk in pd.read_csv(
                self.file_path,
                chunksize=self.chunk_size,
                encoding=encoding,
                sep=separator,
                low_memory=False,
                na_values=['', 'NULL', 'null', 'NA', 'n/a', 'N/A']
            ):
                chunk_count += 1

                # Очистка данных
                chunk = self._clean_dataframe(chunk)

                # Обновление прогресса
                self.update_progress(len(chunk), f"processing_chunk_{chunk_count}")

                self.logger.info(f"Processed chunk {chunk_count} with {len(chunk)} records")

                yield chunk

        except Exception as e:
            self.logger.error(f"Error reading CSV file: {str(e)}")
            raise

    def _detect_encoding(self) -> str:
        """Автоматическое определение кодировки файла"""
        import chardet

        with open(self.file_path, 'rb') as f:
            sample = f.read(10000)  # Читаем первые 10KB
            result = chardet.detect(sample)
            return result['encoding'] or 'utf-8'

    def _detect_separator(self) -> str:
        """Автоматическое определение разделителя CSV"""
        import csv

        with open(self.file_path, 'r', encoding='utf-8') as f:
            sample = f.read(1024)
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample)
                return dialect.delimiter
            except:
                return ','  # По умолчанию запятая

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Очистка и подготовка данных"""

        # Удаление полностью пустых строк
        df = df.dropna(how='all')

        # Очистка названий колонок
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()

        # Конвертация типов данных
        for col in df.columns:
            if df[col].dtype == 'object':
                # Попытка конвертации в datetime
                if self._is_datetime_column(df[col]):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                # Попытка конвертации в numeric
                elif self._is_numeric_column(df[col]):
                    df[col] = pd.to_numeric(df[col], errors='coerce')

        return df

    def _is_datetime_column(self, series: pd.Series) -> bool:
        """Проверка, является ли колонка датой/временем"""
        sample = series.dropna().head(100)
        if len(sample) == 0:
            return False

        try:
            pd.to_datetime(sample, errors='raise')
            return True
        except:
            return False

    def _is_numeric_column(self, series: pd.Series) -> bool:
        """Проверка, является ли колонка числовой"""
        sample = series.dropna().head(100)
        if len(sample) == 0:
            return False

        try:
            pd.to_numeric(sample, errors='raise')
            return True
        except:
            return False
```

## JSON Pipeline

### json_pipeline.py
```python
import dlt
import pandas as pd
import json
from typing import Generator, Dict, Any, List
from .base_pipeline import BasePipeline

class JSONPipeline(BasePipeline):
    """Pipeline для обработки JSON файлов"""

    def __init__(self, job_config: Dict[str, Any]):
        super().__init__(job_config)
        self.chunk_size = job_config.get('chunk_size', 10000)
        self.file_path = f"/app/data/input/{self.filename}"

    def create_pipeline(self) -> dlt.Pipeline:
        """Создание dlt pipeline (аналогично CSV)"""
        return super()._create_pipeline_by_destination()

    @dlt.resource(table_name="json_data")
    def create_resource(self) -> Generator[List[Dict], None, None]:
        """Чтение JSON файла"""

        try:
            # Проверяем формат JSON (массив или JSONL)
            if self._is_jsonl_format():
                yield from self._read_jsonl()
            else:
                yield from self._read_json_array()

        except Exception as e:
            self.logger.error(f"Error reading JSON file: {str(e)}")
            raise

    def _is_jsonl_format(self) -> bool:
        """Проверка, является ли файл JSONL (JSON Lines)"""
        with open(self.file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            try:
                json.loads(first_line)
                second_line = f.readline().strip()
                if second_line:
                    json.loads(second_line)
                    return True  # Два валидных JSON объекта -> JSONL
            except:
                pass
        return False

    def _read_jsonl(self) -> Generator[List[Dict], None, None]:
        """Чтение JSONL файла по частям"""
        batch = []

        with open(self.file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)
                    batch.append(data)

                    if len(batch) >= self.chunk_size:
                        self.update_progress(len(batch), f"processing_batch_{line_num//self.chunk_size}")
                        yield batch
                        batch = []

                except json.JSONDecodeError as e:
                    self.logger.warning(f"Skipping invalid JSON on line {line_num}: {e}")
                    continue

        # Отправка последней партии
        if batch:
            self.update_progress(len(batch), "processing_final_batch")
            yield batch

    def _read_json_array(self) -> Generator[List[Dict], None, None]:
        """Чтение JSON массива"""
        with open(self.file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if isinstance(data, list):
            # Обработка массива по частям
            for i in range(0, len(data), self.chunk_size):
                chunk = data[i:i + self.chunk_size]
                self.update_progress(len(chunk), f"processing_chunk_{i//self.chunk_size + 1}")
                yield chunk
        else:
            # Одиночный объект
            self.update_progress(1, "processing_single_object")
            yield [data]
```

## XML Pipeline

### xml_pipeline.py
```python
import dlt
import pandas as pd
import xml.etree.ElementTree as ET
import xmltodict
from typing import Generator, Dict, Any, List
from .base_pipeline import BasePipeline

class XMLPipeline(BasePipeline):
    """Pipeline для обработки XML файлов"""

    def __init__(self, job_config: Dict[str, Any]):
        super().__init__(job_config)
        self.chunk_size = job_config.get('chunk_size', 1000)  # Меньше для XML
        self.file_path = f"/app/data/input/{self.filename}"

    def create_pipeline(self) -> dlt.Pipeline:
        """Создание dlt pipeline"""
        return super()._create_pipeline_by_destination()

    @dlt.resource(table_name="xml_data")
    def create_resource(self) -> Generator[List[Dict], None, None]:
        """Чтение XML файла"""

        try:
            # Для больших XML файлов используем iterative parsing
            if self._is_large_xml():
                yield from self._read_xml_iterative()
            else:
                yield from self._read_xml_full()

        except Exception as e:
            self.logger.error(f"Error reading XML file: {str(e)}")
            raise

    def _is_large_xml(self) -> bool:
        """Проверка размера XML файла"""
        import os
        file_size = os.path.getsize(self.file_path)
        return file_size > 100 * 1024 * 1024  # Больше 100MB

    def _read_xml_iterative(self) -> Generator[List[Dict], None, None]:
        """Итеративное чтение большого XML файла"""

        batch = []
        context = ET.iterparse(self.file_path, events=('start', 'end'))
        context = iter(context)
        event, root = next(context)

        for event, elem in context:
            if event == 'end':
                # Предполагаем, что каждый элемент верхнего уровня - это запись
                if elem.tag != root.tag:
                    record = self._xml_element_to_dict(elem)
                    batch.append(record)

                    if len(batch) >= self.chunk_size:
                        self.update_progress(len(batch))
                        yield batch
                        batch = []

                    # Очистка памяти
                    elem.clear()
                    root.clear()

        # Отправка последней партии
        if batch:
            self.update_progress(len(batch))
            yield batch

    def _read_xml_full(self) -> Generator[List[Dict], None, None]:
        """Полное чтение XML файла"""

        with open(self.file_path, 'r', encoding='utf-8') as f:
            xml_data = xmltodict.parse(f.read())

        # Извлечение данных в зависимости от структуры
        data_list = self._extract_records_from_xml(xml_data)

        # Обработка по частям
        for i in range(0, len(data_list), self.chunk_size):
            chunk = data_list[i:i + self.chunk_size]
            self.update_progress(len(chunk))
            yield chunk

    def _xml_element_to_dict(self, element) -> Dict[str, Any]:
        """Конвертация XML элемента в словарь"""
        result = {}

        # Атрибуты элемента
        if element.attrib:
            result.update({f"@{k}": v for k, v in element.attrib.items()})

        # Текст элемента
        if element.text and element.text.strip():
            result['#text'] = element.text.strip()

        # Дочерние элементы
        for child in element:
            child_data = self._xml_element_to_dict(child)
            if child.tag in result:
                # Если элемент уже существует, делаем список
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data

        return result

    def _extract_records_from_xml(self, xml_data: Dict) -> List[Dict]:
        """Извлечение списка записей из XML структуры"""

        # Пытаемся найти массив данных в XML
        def find_array_in_dict(data, level=0):
            if level > 5:  # Ограничение глубины поиска
                return []

            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                for value in data.values():
                    if isinstance(value, list) and len(value) > 0:
                        return value
                    elif isinstance(value, dict):
                        result = find_array_in_dict(value, level + 1)
                        if result:
                            return result
            return []

        records = find_array_in_dict(xml_data)

        if not records:
            # Если не найден массив, возвращаем корневой элемент как единственную запись
            return [xml_data]

        return records
```

## Главный модуль dlt-worker

### main.py
```python
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any
import json
import os

from pipelines.csv_pipeline import CSVPipeline
from pipelines.json_pipeline import JSONPipeline
from pipelines.xml_pipeline import XMLPipeline

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/dlt-worker.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class DLTWorker:
    """Основной класс для выполнения ETL задач через dlt"""

    def __init__(self):
        self.pipeline_classes = {
            'csv': CSVPipeline,
            'json': JSONPipeline,
            'xml': XMLPipeline
        }

    async def process_job(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Обработка одной задачи ETL"""

        job_id = job_config['job_id']
        file_format = job_config['file_format']

        logger.info(f"Starting job {job_id} for {file_format} file")

        try:
            # Выбор нужного pipeline
            if file_format not in self.pipeline_classes:
                raise ValueError(f"Unsupported file format: {file_format}")

            pipeline_class = self.pipeline_classes[file_format]
            pipeline = pipeline_class(job_config)

            # Установка callback для прогресса
            pipeline.set_progress_callback(self._progress_callback)

            # Выполнение ETL
            result = pipeline.run()

            logger.info(f"Job {job_id} completed successfully")
            return result

        except Exception as e:
            logger.error(f"Job {job_id} failed: {str(e)}", exc_info=True)
            return {
                'job_id': job_id,
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }

    def _progress_callback(self, progress_data: Dict[str, Any]):
        """Callback для отслеживания прогресса"""

        # Отправка прогресса в backend через HTTP API
        try:
            import requests

            requests.post(
                'http://backend:8000/api/v1/progress',
                json=progress_data,
                timeout=5
            )
        except Exception as e:
            logger.warning(f"Failed to send progress update: {e}")

if __name__ == "__main__":
    # Получение конфигурации задачи из переменных окружения или файла
    job_config_str = os.environ.get('DLT_JOB_CONFIG')

    if not job_config_str:
        logger.error("No job configuration provided")
        exit(1)

    try:
        job_config = json.loads(job_config_str)

        worker = DLTWorker()
        result = asyncio.run(worker.process_job(job_config))

        logger.info(f"Job result: {result}")

        # Возвращаем код ошибки для контейнера
        exit(0 if result.get('status') != 'failed' else 1)

    except Exception as e:
        logger.error(f"Worker failed to start: {str(e)}", exc_info=True)
        exit(1)
```

## Задачи для выполнения

### 1. Базовая настройка dlt-worker
- [ ] Создать Dockerfile с установкой dlt и зависимостей
- [ ] Настроить requirements.txt со всеми необходимыми пакетами
- [ ] Реализовать базовый класс BasePipeline

### 2. Реализация pipeline'ов для форматов файлов
- [ ] CSVPipeline с поддержкой streaming чтения
- [ ] JSONPipeline для JSON и JSONL файлов
- [ ] XMLPipeline с итеративной обработкой

### 3. Конфигурация destinations
- [ ] Настройка PostgreSQL destination
- [ ] Настройка ClickHouse destination
- [ ] Настройка HDFS/Filesystem destination

### 4. Обработка данных и очистка
- [ ] Автоматическое определение типов данных
- [ ] Очистка и нормализация данных
- [ ] Обработка ошибок в данных

### 5. Мониторинг и прогресс
- [ ] Система отслеживания прогресса
- [ ] Интеграция с backend API для уведомлений
- [ ] Логирование выполнения

### 6. Интеграция с Airflow
- [ ] Адаптация для запуска из Airflow DAG'ов
- [ ] Передача конфигурации через параметры
- [ ] Обработка результатов выполнения

## Ожидаемый результат
После выполнения этой задачи должен быть готов dlt-worker для обработки больших файлов в различных форматах с загрузкой в разные типы хранилищ.

## Следующая задача
[06_websocket_realtime.md](./06_websocket_realtime.md) - Настройка WebSocket для real-time обновлений