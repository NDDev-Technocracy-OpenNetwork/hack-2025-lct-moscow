# Задача 4: FastAPI Backend Development

## Цель
Разработать Backend API на FastAPI для управления процессом загрузки и обработки файлов с поддержкой WebSocket для real-time обновлений.

## Технические требования
- FastAPI с async/await поддержкой
- REST API для управления задачами
- WebSocket для real-time уведомлений
- Интеграция с Airflow для запуска DAG'ов
- Файловые операции для больших файлов (до 10GB)
- Генерация DDL/ETL скриптов

## Структура Backend проекта

```
services/backend/
├── Dockerfile
├── requirements.txt
├── app/
│   ├── main.py
│   ├── config.py
│   ├── database.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── jobs.py
│   │   ├── files.py
│   │   └── destinations.py
│   ├── api/
│   │   ├── __init__.py
│   │   ├── v1/
│   │   │   ├── __init__.py
│   │   │   ├── jobs.py
│   │   │   ├── files.py
│   │   │   ├── destinations.py
│   │   │   └── websocket.py
│   │   └── deps.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── file_processor.py
│   │   ├── ddl_generator.py
│   │   ├── airflow_client.py
│   │   └── websocket_manager.py
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── jobs.py
│   │   ├── files.py
│   │   └── destinations.py
│   └── utils/
│       ├── __init__.py
│       ├── file_analyzer.py
│       └── validators.py
```

## Основные файлы

### Dockerfile
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Системные зависимости
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Python зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование приложения
COPY app/ ./app/

# Создание директорий для данных
RUN mkdir -p /app/data/input /app/data/temp /app/data/processed

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
```

### requirements.txt
```txt
fastapi==0.104.1
uvicorn[standard]==0.24.0
sqlalchemy==2.0.23
psycopg2-binary==2.9.7
alembic==1.12.1
pydantic==2.5.0
python-multipart==0.0.6
aiofiles==23.2.1
pandas==2.1.3
pyarrow==14.0.1
apache-airflow-client==1.0.0
websockets==12.0
redis==5.0.1
celery==5.3.4
python-json-logger==2.0.7
```

### main.py
```python
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from app.api.v1 import jobs, files, destinations, websocket
from app.core.websocket_manager import WebSocketManager
from app.database import engine
from app.models import Base

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Глобальный WebSocket manager
websocket_manager = WebSocketManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Создание таблиц БД при старте
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables created")
    yield
    # Cleanup при остановке
    logger.info("Application shutdown")

app = FastAPI(
    title="BigData Processing API",
    description="API for processing large files with multiple destinations",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Включение роутеров
app.include_router(jobs.router, prefix="/api/v1/jobs", tags=["jobs"])
app.include_router(files.router, prefix="/api/v1/files", tags=["files"])
app.include_router(destinations.router, prefix="/api/v1/destinations", tags=["destinations"])

# WebSocket endpoint
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await websocket_manager.connect(websocket, client_id)
    try:
        while True:
            # Поддержание соединения
            data = await websocket.receive_text()
            # Эхо для проверки соединения
            await websocket_manager.send_personal_message(f"Echo: {data}", client_id)
    except WebSocketDisconnect:
        websocket_manager.disconnect(client_id)

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "bigdata-backend"}

# Notification endpoint для Airflow callbacks
@app.post("/api/v1/notify")
async def airflow_notification(notification: dict):
    """Endpoint для получения уведомлений от Airflow"""
    job_id = notification.get('job_id')
    if job_id:
        await websocket_manager.broadcast_to_job(job_id, notification)
    return {"status": "notification sent"}

# Сделать websocket_manager доступным в других модулях
app.state.websocket_manager = websocket_manager
```

### models/jobs.py
```python
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Text, JSON
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
import uuid
from app.database import Base

class ProcessingJob(Base):
    __tablename__ = "processing_jobs"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(UUID(as_uuid=True), unique=True, index=True, default=uuid.uuid4)
    filename = Column(String(255), nullable=False)
    file_size = Column(BigInteger, nullable=False)
    file_format = Column(String(50), nullable=False)
    destination_type = Column(String(50), nullable=False)
    destination_config = Column(JSON, nullable=False)
    status = Column(String(50), nullable=False, default='pending')
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(Text, nullable=True)
    records_processed = Column(BigInteger, default=0)
    records_total = Column(BigInteger, nullable=True)
    ddl_script = Column(Text, nullable=True)
    etl_script = Column(Text, nullable=True)

class ProcessingProgress(Base):
    __tablename__ = "processing_progress"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    step_name = Column(String(100), nullable=False)
    step_status = Column(String(50), nullable=False)
    records_processed = Column(BigInteger, default=0)
    total_records = Column(BigInteger, nullable=True)
    progress_percentage = Column(String(10), default="0.00")
    step_details = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
```

### api/v1/jobs.py
```python
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
import aiofiles
import os
import uuid
from datetime import datetime

from app.database import get_db
from app.models.jobs import ProcessingJob
from app.schemas.jobs import JobCreate, JobResponse, JobStatus
from app.core.file_processor import FileProcessor
from app.core.ddl_generator import DDLGenerator
from app.core.airflow_client import AirflowClient

router = APIRouter()

@router.post("/upload", response_model=JobResponse)
async def upload_file(
    file: UploadFile = File(...),
    destination_type: str = Form(...),
    destination_config: str = Form(...),
    db: AsyncSession = Depends(get_db)
):
    """Загрузка файла и создание задачи обработки"""

    # Валидация файла
    if file.size > 10 * 1024 * 1024 * 1024:  # 10GB
        raise HTTPException(status_code=413, detail="File too large")

    allowed_formats = ['csv', 'json', 'xml']
    file_format = file.filename.split('.')[-1].lower()
    if file_format not in allowed_formats:
        raise HTTPException(status_code=400, detail=f"Unsupported format: {file_format}")

    # Сохранение файла
    job_id = uuid.uuid4()
    file_path = f"/app/data/input/{job_id}_{file.filename}"

    async with aiofiles.open(file_path, 'wb') as f:
        content = await file.read()
        await f.write(content)

    # Создание записи в БД
    import json
    destination_config_dict = json.loads(destination_config)

    job = ProcessingJob(
        job_id=job_id,
        filename=file.filename,
        file_size=file.size,
        file_format=file_format,
        destination_type=destination_type,
        destination_config=destination_config_dict,
        status='pending'
    )

    db.add(job)
    await db.commit()
    await db.refresh(job)

    # Анализ файла и генерация DDL
    try:
        file_processor = FileProcessor()
        schema_info = await file_processor.analyze_file(file_path, file_format)

        ddl_generator = DDLGenerator()
        ddl_script = ddl_generator.generate_ddl(schema_info, destination_type, destination_config_dict)
        etl_script = ddl_generator.generate_etl_script(schema_info, destination_type)

        # Обновление DDL скриптов в БД
        job.ddl_script = ddl_script
        job.etl_script = etl_script
        await db.commit()

    except Exception as e:
        job.status = 'failed'
        job.error_message = str(e)
        await db.commit()
        raise HTTPException(status_code=500, detail=f"File analysis failed: {str(e)}")

    # Запуск DAG в Airflow
    try:
        airflow_client = AirflowClient()
        dag_run_id = await airflow_client.trigger_dag(job_id, {
            'job_id': str(job_id),
            'filename': file.filename,
            'file_format': file_format,
            'destination_type': destination_type,
            'destination_config': destination_config_dict
        })

        job.status = 'processing'
        job.started_at = datetime.utcnow()
        await db.commit()

    except Exception as e:
        job.status = 'failed'
        job.error_message = f"Failed to start Airflow DAG: {str(e)}"
        await db.commit()
        raise HTTPException(status_code=500, detail="Failed to start processing")

    return JobResponse.from_orm(job)

@router.get("/", response_model=List[JobResponse])
async def get_jobs(db: AsyncSession = Depends(get_db)):
    """Получение списка всех задач"""
    from sqlalchemy import select

    result = await db.execute(select(ProcessingJob).order_by(ProcessingJob.created_at.desc()))
    jobs = result.scalars().all()
    return [JobResponse.from_orm(job) for job in jobs]

@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, db: AsyncSession = Depends(get_db)):
    """Получение информации о конкретной задаче"""
    from sqlalchemy import select

    result = await db.execute(select(ProcessingJob).where(ProcessingJob.job_id == job_id))
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobResponse.from_orm(job)

@router.get("/{job_id}/status", response_model=JobStatus)
async def get_job_status(job_id: str, db: AsyncSession = Depends(get_db)):
    """Получение статуса задачи"""
    from sqlalchemy import select

    result = await db.execute(select(ProcessingJob).where(ProcessingJob.job_id == job_id))
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobStatus(
        job_id=str(job.job_id),
        status=job.status,
        records_processed=job.records_processed or 0,
        records_total=job.records_total or 0,
        progress_percentage=round((job.records_processed or 0) / max(job.records_total or 1, 1) * 100, 2)
    )

@router.delete("/{job_id}")
async def cancel_job(job_id: str, db: AsyncSession = Depends(get_db)):
    """Отмена выполнения задачи"""
    # Логика отмены через Airflow API
    airflow_client = AirflowClient()
    await airflow_client.cancel_dag_run(job_id)

    # Обновление статуса в БД
    from sqlalchemy import select, update

    await db.execute(
        update(ProcessingJob)
        .where(ProcessingJob.job_id == job_id)
        .values(status='cancelled')
    )
    await db.commit()

    return {"message": "Job cancelled successfully"}
```

### core/websocket_manager.py
```python
from fastapi import WebSocket
from typing import List, Dict
import json

class WebSocketManager:
    def __init__(self):
        # Активные соединения по client_id
        self.active_connections: Dict[str, WebSocket] = {}
        # Маппинг job_id -> список client_id
        self.job_subscriptions: Dict[str, List[str]] = {}

    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        self.active_connections[client_id] = websocket

    def disconnect(self, client_id: str):
        if client_id in self.active_connections:
            del self.active_connections[client_id]

        # Удаление из подписок на задачи
        for job_id, subscribers in self.job_subscriptions.items():
            if client_id in subscribers:
                subscribers.remove(client_id)

    async def send_personal_message(self, message: str, client_id: str):
        if client_id in self.active_connections:
            websocket = self.active_connections[client_id]
            await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections.values():
            await connection.send_text(message)

    async def subscribe_to_job(self, client_id: str, job_id: str):
        """Подписка клиента на обновления конкретной задачи"""
        if job_id not in self.job_subscriptions:
            self.job_subscriptions[job_id] = []

        if client_id not in self.job_subscriptions[job_id]:
            self.job_subscriptions[job_id].append(client_id)

    async def broadcast_to_job(self, job_id: str, message: dict):
        """Отправка сообщения всем подписчикам задачи"""
        if job_id in self.job_subscriptions:
            message_str = json.dumps(message)
            for client_id in self.job_subscriptions[job_id]:
                await self.send_personal_message(message_str, client_id)
```

## Задачи для выполнения

### 1. Базовая настройка FastAPI
- [ ] Создать Dockerfile и requirements.txt
- [ ] Настроить структуру проекта
- [ ] Реализовать main.py с базовой конфигурацией
- [ ] Добавить CORS и middleware

### 2. Модели и схемы данных
- [ ] Создать SQLAlchemy модели для задач и прогресса
- [ ] Реализовать Pydantic схемы для API
- [ ] Настроить подключение к PostgreSQL

### 3. API endpoints для управления задачами
- [ ] POST /jobs/upload - загрузка файлов
- [ ] GET /jobs - список задач
- [ ] GET /jobs/{id} - информация о задаче
- [ ] GET /jobs/{id}/status - статус задачи
- [ ] DELETE /jobs/{id} - отмена задачи

### 4. WebSocket для real-time обновлений
- [ ] Реализовать WebSocketManager
- [ ] Создать endpoints для WebSocket соединений
- [ ] Добавить систему подписок на обновления задач

### 5. Интеграция с Airflow
- [ ] Создать AirflowClient для управления DAG'ами
- [ ] Реализовать триггеры для запуска обработки
- [ ] Добавить callback endpoints для получения уведомлений

### 6. Обработка файлов и генерация скриптов
- [ ] FileProcessor для анализа структуры файлов
- [ ] DDLGenerator для создания DDL скриптов
- [ ] Поддержка всех форматов файлов (CSV, JSON, XML)

### 7. Тестирование и отладка
- [ ] Unit тесты для основных компонентов
- [ ] Тестирование WebSocket соединений
- [ ] Интеграционные тесты с Airflow

## Ожидаемый результат
После выполнения этой задачи должен быть готов полнофункциональный Backend API с поддержкой загрузки файлов, WebSocket уведомлений и интеграцией с Airflow.

## Следующая задача
[05_dlt_integration.md](./05_dlt_integration.md) - Интеграция с dlt для ETL процессов