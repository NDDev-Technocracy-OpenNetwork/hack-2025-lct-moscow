# Задача 6: WebSocket Real-time Updates

## Цель
Реализация WebSocket соединений для отправки real-time обновлений статуса обработки файлов и визуализации прогресса в React frontend.

## Технические требования
- WebSocket соединения для real-time коммуникации
- Система подписок на обновления конкретных задач
- Интеграция с Airflow для получения callback'ов
- Отправка прогресса обработки и метрик
- Обработка переподключений и ошибок

## Компоненты системы

### Backend WebSocket Manager (уже частично реализован в задаче 4)

#### Расширение WebSocketManager

```python
# app/core/websocket_manager.py (дополнение к существующему коду)

import json
import asyncio
from typing import Dict, List, Optional, Callable
from fastapi import WebSocket
from datetime import datetime
import uuid

class WebSocketManager:
    def __init__(self):
        # Активные соединения по client_id
        self.active_connections: Dict[str, WebSocket] = {}
        # Маппинг job_id -> список client_id
        self.job_subscriptions: Dict[str, List[str]] = {}
        # Кеш последних сообщений для каждой задачи
        self.message_cache: Dict[str, List[Dict]] = {}
        # Callback'и для различных типов событий
        self.event_handlers: Dict[str, List[Callable]] = {}

    async def connect(self, websocket: WebSocket, client_id: str):
        """Подключение нового клиента"""
        await websocket.accept()
        self.active_connections[client_id] = websocket

        # Отправка приветственного сообщения
        await self.send_personal_message(json.dumps({
            "type": "connection_established",
            "client_id": client_id,
            "timestamp": datetime.utcnow().isoformat()
        }), client_id)

    def disconnect(self, client_id: str):
        """Отключение клиента"""
        if client_id in self.active_connections:
            del self.active_connections[client_id]

        # Удаление из подписок на задачи
        for job_id, subscribers in self.job_subscriptions.items():
            if client_id in subscribers:
                subscribers.remove(client_id)

    async def send_personal_message(self, message: str, client_id: str):
        """Отправка персонального сообщения"""
        if client_id in self.active_connections:
            try:
                websocket = self.active_connections[client_id]
                await websocket.send_text(message)
            except Exception as e:
                # Соединение разорвано, удаляем клиента
                self.disconnect(client_id)

    async def broadcast(self, message: str):
        """Широковещательная отправка всем клиентам"""
        disconnected_clients = []

        for client_id, websocket in self.active_connections.items():
            try:
                await websocket.send_text(message)
            except:
                disconnected_clients.append(client_id)

        # Удаление отключившихся клиентов
        for client_id in disconnected_clients:
            self.disconnect(client_id)

    async def subscribe_to_job(self, client_id: str, job_id: str):
        """Подписка клиента на обновления задачи"""
        if job_id not in self.job_subscriptions:
            self.job_subscriptions[job_id] = []

        if client_id not in self.job_subscriptions[job_id]:
            self.job_subscriptions[job_id].append(client_id)

        # Отправка кешированных сообщений для задачи
        if job_id in self.message_cache:
            for cached_message in self.message_cache[job_id]:
                await self.send_personal_message(
                    json.dumps(cached_message), client_id
                )

    async def unsubscribe_from_job(self, client_id: str, job_id: str):
        """Отписка от обновлений задачи"""
        if job_id in self.job_subscriptions:
            if client_id in self.job_subscriptions[job_id]:
                self.job_subscriptions[job_id].remove(client_id)

    async def broadcast_to_job(self, job_id: str, message: Dict):
        """Отправка сообщения всем подписчикам задачи"""
        # Добавление сообщения в кеш
        if job_id not in self.message_cache:
            self.message_cache[job_id] = []

        message['timestamp'] = datetime.utcnow().isoformat()
        self.message_cache[job_id].append(message)

        # Ограничение размера кеша (последние 100 сообщений)
        if len(self.message_cache[job_id]) > 100:
            self.message_cache[job_id] = self.message_cache[job_id][-100:]

        # Отправка подписчикам
        if job_id in self.job_subscriptions:
            message_str = json.dumps(message)
            disconnected_clients = []

            for client_id in self.job_subscriptions[job_id]:
                try:
                    await self.send_personal_message(message_str, client_id)
                except:
                    disconnected_clients.append(client_id)

            # Удаление отключившихся клиентов
            for client_id in disconnected_clients:
                self.job_subscriptions[job_id].remove(client_id)

    async def send_job_progress(self, job_id: str, progress_data: Dict):
        """Отправка прогресса выполнения задачи"""
        message = {
            "type": "job_progress",
            "job_id": job_id,
            "progress": progress_data
        }
        await self.broadcast_to_job(job_id, message)

    async def send_job_status_change(self, job_id: str, old_status: str, new_status: str, details: Optional[Dict] = None):
        """Отправка изменения статуса задачи"""
        message = {
            "type": "job_status_change",
            "job_id": job_id,
            "old_status": old_status,
            "new_status": new_status,
            "details": details or {}
        }
        await self.broadcast_to_job(job_id, message)

    async def send_error_notification(self, job_id: str, error: str, stage: str):
        """Отправка уведомления об ошибке"""
        message = {
            "type": "job_error",
            "job_id": job_id,
            "error": error,
            "stage": stage
        }
        await self.broadcast_to_job(job_id, message)

    def get_connection_stats(self) -> Dict:
        """Получение статистики подключений"""
        return {
            "active_connections": len(self.active_connections),
            "job_subscriptions": len(self.job_subscriptions),
            "total_subscriptions": sum(len(subs) for subs in self.job_subscriptions.values()),
            "cached_messages": sum(len(msgs) for msgs in self.message_cache.values())
        }
```

### WebSocket API endpoints

```python
# app/api/v1/websocket.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from app.core.websocket_manager import WebSocketManager
import json
import uuid

router = APIRouter()

# Предполагается, что websocket_manager доступен через dependency
async def get_websocket_manager() -> WebSocketManager:
    # Возвращается глобальный экземпляр из main.py
    from app.main import websocket_manager
    return websocket_manager

@router.websocket("/connect")
async def websocket_connect(
    websocket: WebSocket,
    manager: WebSocketManager = Depends(get_websocket_manager)
):
    """Основное WebSocket соединение"""
    client_id = str(uuid.uuid4())

    await manager.connect(websocket, client_id)

    try:
        while True:
            # Получение сообщений от клиента
            data = await websocket.receive_text()

            try:
                message = json.loads(data)
                await handle_client_message(message, client_id, manager)
            except json.JSONDecodeError:
                # Эхо для простых сообщений
                await manager.send_personal_message(f"Echo: {data}", client_id)

    except WebSocketDisconnect:
        manager.disconnect(client_id)

async def handle_client_message(message: dict, client_id: str, manager: WebSocketManager):
    """Обработка сообщений от клиента"""

    message_type = message.get('type')

    if message_type == 'subscribe_job':
        job_id = message.get('job_id')
        if job_id:
            await manager.subscribe_to_job(client_id, job_id)
            await manager.send_personal_message(
                json.dumps({
                    "type": "subscription_confirmed",
                    "job_id": job_id
                }),
                client_id
            )

    elif message_type == 'unsubscribe_job':
        job_id = message.get('job_id')
        if job_id:
            await manager.unsubscribe_from_job(client_id, job_id)

    elif message_type == 'get_job_status':
        job_id = message.get('job_id')
        if job_id:
            # Получение текущего статуса из БД и отправка клиенту
            status = await get_job_current_status(job_id)
            await manager.send_personal_message(
                json.dumps({
                    "type": "job_status",
                    "job_id": job_id,
                    "status": status
                }),
                client_id
            )

    elif message_type == 'ping':
        # Pong для проверки соединения
        await manager.send_personal_message(
            json.dumps({"type": "pong"}),
            client_id
        )

async def get_job_current_status(job_id: str) -> dict:
    """Получение текущего статуса задачи из БД"""
    # Здесь должен быть код получения статуса из БД
    # Для примера возвращаем заглушку
    return {
        "status": "processing",
        "progress": 45.5,
        "records_processed": 1000,
        "records_total": 2200
    }
```

### Интеграция с Backend API для прогресса

```python
# app/api/v1/progress.py (новый файл)

from fastapi import APIRouter, Depends, BackgroundTasks
from app.core.websocket_manager import WebSocketManager
from pydantic import BaseModel
from typing import Optional
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

class ProgressUpdate(BaseModel):
    job_id: str
    stage: str
    records_processed: int
    records_total: Optional[int] = None
    percentage: Optional[float] = None
    details: Optional[dict] = None

class StatusUpdate(BaseModel):
    job_id: str
    old_status: str
    new_status: str
    details: Optional[dict] = None

async def get_websocket_manager() -> WebSocketManager:
    from app.main import websocket_manager
    return websocket_manager

@router.post("/progress")
async def update_progress(
    progress: ProgressUpdate,
    background_tasks: BackgroundTasks,
    manager: WebSocketManager = Depends(get_websocket_manager)
):
    """Endpoint для получения обновлений прогресса от dlt-worker"""

    try:
        # Отправка прогресса через WebSocket
        background_tasks.add_task(
            manager.send_job_progress,
            progress.job_id,
            progress.dict(exclude={'job_id'})
        )

        # Обновление в БД (если необходимо)
        background_tasks.add_task(
            update_progress_in_db,
            progress
        )

        return {"status": "progress updated"}

    except Exception as e:
        logger.error(f"Failed to update progress: {e}")
        return {"status": "error", "message": str(e)}

@router.post("/status")
async def update_status(
    status: StatusUpdate,
    background_tasks: BackgroundTasks,
    manager: WebSocketManager = Depends(get_websocket_manager)
):
    """Endpoint для получения обновлений статуса от Airflow"""

    try:
        # Отправка изменения статуса через WebSocket
        background_tasks.add_task(
            manager.send_job_status_change,
            status.job_id,
            status.old_status,
            status.new_status,
            status.details
        )

        # Обновление в БД
        background_tasks.add_task(
            update_status_in_db,
            status
        )

        return {"status": "status updated"}

    except Exception as e:
        logger.error(f"Failed to update status: {e}")
        return {"status": "error", "message": str(e)}

async def update_progress_in_db(progress: ProgressUpdate):
    """Обновление прогресса в базе данных"""
    # Здесь должен быть код обновления БД
    pass

async def update_status_in_db(status: StatusUpdate):
    """Обновление статуса в базе данных"""
    # Здесь должен быть код обновления БД
    pass
```

## Frontend WebSocket клиент

### WebSocket Hook для React

```typescript
// frontend/src/hooks/useWebSocket.ts

import { useEffect, useRef, useState, useCallback } from 'react';

export interface WebSocketMessage {
  type: string;
  job_id?: string;
  timestamp?: string;
  [key: string]: any;
}

export interface UseWebSocketReturn {
  isConnected: boolean;
  sendMessage: (message: any) => void;
  subscribeToJob: (jobId: string) => void;
  unsubscribeFromJob: (jobId: string) => void;
  lastMessage: WebSocketMessage | null;
  connectionError: string | null;
}

export const useWebSocket = (
  url: string,
  onMessage?: (message: WebSocketMessage) => void,
  autoReconnect: boolean = true
): UseWebSocketReturn => {
  const [isConnected, setIsConnected] = useState(false);
  const [lastMessage, setLastMessage] = useState<WebSocketMessage | null>(null);
  const [connectionError, setConnectionError] = useState<string | null>(null);

  const websocketRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const reconnectAttempts = useRef(0);
  const maxReconnectAttempts = 5;
  const reconnectInterval = 3000;

  const connect = useCallback(() => {
    try {
      const websocket = new WebSocket(url);

      websocket.onopen = () => {
        console.log('WebSocket connected');
        setIsConnected(true);
        setConnectionError(null);
        reconnectAttempts.current = 0;
      };

      websocket.onclose = (event) => {
        console.log('WebSocket disconnected:', event.code, event.reason);
        setIsConnected(false);
        websocketRef.current = null;

        // Автоматическое переподключение
        if (autoReconnect && reconnectAttempts.current < maxReconnectAttempts) {
          reconnectTimeoutRef.current = setTimeout(() => {
            reconnectAttempts.current++;
            console.log(`Reconnection attempt ${reconnectAttempts.current}`);
            connect();
          }, reconnectInterval);
        }
      };

      websocket.onerror = (error) => {
        console.error('WebSocket error:', error);
        setConnectionError('Connection error occurred');
      };

      websocket.onmessage = (event) => {
        try {
          const message: WebSocketMessage = JSON.parse(event.data);
          setLastMessage(message);

          if (onMessage) {
            onMessage(message);
          }
        } catch (error) {
          console.error('Error parsing WebSocket message:', error);
        }
      };

      websocketRef.current = websocket;

    } catch (error) {
      console.error('Error creating WebSocket connection:', error);
      setConnectionError('Failed to create connection');
    }
  }, [url, onMessage, autoReconnect]);

  const sendMessage = useCallback((message: any) => {
    if (websocketRef.current && websocketRef.current.readyState === WebSocket.OPEN) {
      websocketRef.current.send(JSON.stringify(message));
    } else {
      console.warn('WebSocket is not connected');
    }
  }, []);

  const subscribeToJob = useCallback((jobId: string) => {
    sendMessage({
      type: 'subscribe_job',
      job_id: jobId
    });
  }, [sendMessage]);

  const unsubscribeFromJob = useCallback((jobId: string) => {
    sendMessage({
      type: 'unsubscribe_job',
      job_id: jobId
    });
  }, [sendMessage]);

  useEffect(() => {
    connect();

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }

      if (websocketRef.current) {
        websocketRef.current.close();
      }
    };
  }, [connect]);

  return {
    isConnected,
    sendMessage,
    subscribeToJob,
    unsubscribeFromJob,
    lastMessage,
    connectionError
  };
};
```

### WebSocket контекст для React приложения

```typescript
// frontend/src/contexts/WebSocketContext.tsx

import React, { createContext, useContext, useEffect, useState } from 'react';
import { useWebSocket, WebSocketMessage } from '../hooks/useWebSocket';

interface JobProgress {
  job_id: string;
  status: string;
  stage: string;
  records_processed: number;
  records_total?: number;
  percentage?: number;
  timestamp: string;
}

interface WebSocketContextType {
  isConnected: boolean;
  subscribeToJob: (jobId: string) => void;
  unsubscribeFromJob: (jobId: string) => void;
  getJobProgress: (jobId: string) => JobProgress | null;
  connectionError: string | null;
}

const WebSocketContext = createContext<WebSocketContextType | null>(null);

export const useWebSocketContext = () => {
  const context = useContext(WebSocketContext);
  if (!context) {
    throw new Error('useWebSocketContext must be used within WebSocketProvider');
  }
  return context;
};

interface WebSocketProviderProps {
  children: React.ReactNode;
  websocketUrl: string;
}

export const WebSocketProvider: React.FC<WebSocketProviderProps> = ({
  children,
  websocketUrl
}) => {
  const [jobProgressMap, setJobProgressMap] = useState<Map<string, JobProgress>>(new Map());

  const handleMessage = (message: WebSocketMessage) => {
    switch (message.type) {
      case 'job_progress':
        if (message.job_id) {
          setJobProgressMap(prev => {
            const newMap = new Map(prev);
            newMap.set(message.job_id!, {
              job_id: message.job_id!,
              status: message.progress?.status || 'processing',
              stage: message.progress?.stage || '',
              records_processed: message.progress?.records_processed || 0,
              records_total: message.progress?.records_total,
              percentage: message.progress?.percentage,
              timestamp: message.timestamp || new Date().toISOString()
            });
            return newMap;
          });
        }
        break;

      case 'job_status_change':
        if (message.job_id) {
          setJobProgressMap(prev => {
            const newMap = new Map(prev);
            const existing = newMap.get(message.job_id!) || {
              job_id: message.job_id!,
              status: message.new_status,
              stage: '',
              records_processed: 0,
              timestamp: message.timestamp || new Date().toISOString()
            };

            newMap.set(message.job_id!, {
              ...existing,
              status: message.new_status
            });
            return newMap;
          });
        }
        break;

      case 'job_error':
        if (message.job_id) {
          setJobProgressMap(prev => {
            const newMap = new Map(prev);
            const existing = newMap.get(message.job_id!) || {
              job_id: message.job_id!,
              status: 'failed',
              stage: message.stage || '',
              records_processed: 0,
              timestamp: message.timestamp || new Date().toISOString()
            };

            newMap.set(message.job_id!, {
              ...existing,
              status: 'failed'
            });
            return newMap;
          });
        }
        break;
    }
  };

  const {
    isConnected,
    subscribeToJob,
    unsubscribeFromJob,
    connectionError
  } = useWebSocket(websocketUrl, handleMessage);

  const getJobProgress = (jobId: string): JobProgress | null => {
    return jobProgressMap.get(jobId) || null;
  };

  return (
    <WebSocketContext.Provider value={{
      isConnected,
      subscribeToJob,
      unsubscribeFromJob,
      getJobProgress,
      connectionError
    }}>
      {children}
    </WebSocketContext.Provider>
  );
};
```

## Задачи для выполнения

### 1. Расширение Backend WebSocket функциональности
- [ ] Доработать WebSocketManager с поддержкой подписок
- [ ] Добавить кеширование сообщений для переподключений
- [ ] Реализовать обработку различных типов событий
- [ ] Добавить статистику соединений

### 2. API endpoints для WebSocket взаимодействия
- [ ] Endpoint для WebSocket соединений
- [ ] Обработка команд от клиентов (подписка/отписка)
- [ ] API для получения прогресса от dlt-worker
- [ ] API для получения уведомлений от Airflow

### 3. Frontend WebSocket клиент
- [ ] React Hook для WebSocket соединений
- [ ] Автоматическое переподключение при разрыве
- [ ] Обработка различных типов сообщений
- [ ] Context для управления состоянием WebSocket

### 4. Интеграция с другими компонентами
- [ ] Обновление dlt-worker для отправки прогресса
- [ ] Модификация Airflow операторов для уведомлений
- [ ] Обновление React компонентов для получения real-time данных

### 5. Обработка ошибок и надежность
- [ ] Graceful handling отключений
- [ ] Retry логика для неудачных соединений
- [ ] Валидация сообщений WebSocket
- [ ] Logging и мониторинг соединений

### 6. Тестирование WebSocket функциональности
- [ ] Unit тесты для WebSocketManager
- [ ] Интеграционные тесты WebSocket endpoints
- [ ] E2E тесты real-time обновлений
- [ ] Тесты производительности для множественных соединений

## Ожидаемый результат
После выполнения этой задачи должна быть готова полная система real-time обновлений через WebSocket с поддержкой подписок, переподключений и интеграции со всеми компонентами системы.

## Следующая задача
[07_react_ui_setup.md](./07_react_ui_setup.md) - Настройка React UI приложения