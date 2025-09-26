import socketio
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class SocketIOManager:
    def __init__(self):
        self.sio = socketio.AsyncServer(
            cors_allowed_origins="*",
            logger=False,
            engineio_logger=False
        )
        self.clients: Dict[str, str] = {}

    async def connect(self, sid: str, environ: dict):
        logger.info(f"Client {sid} connected")
        await self.sio.emit('connected', {'message': 'Connected successfully'}, to=sid)

    async def disconnect(self, sid: str):
        if sid in self.clients:
            client_id = self.clients[sid]
            del self.clients[sid]
            logger.info(f"Client {client_id} ({sid}) disconnected")
        else:
            logger.info(f"Client {sid} disconnected")

    async def send_job_update(self, job_id: str, data: dict):
        await self.sio.emit('job_update', {
            'job_id': job_id,
            'data': data
        })
        logger.debug(f"Sent job update for {job_id}: {data}")

    async def send_progress_update(self, job_id: str, progress: dict):
        await self.sio.emit('progress_update', {
            'job_id': job_id,
            'progress': progress
        })
        logger.debug(f"Sent progress update for {job_id}: {progress}")

    def register_client(self, sid: str, client_id: str):
        self.clients[sid] = client_id
        logger.info(f"Registered client {client_id} with session {sid}")

    def get_connection_stats(self) -> dict:
        return {
            "total_connections": len(self.clients),
            "clients": list(self.clients.values())
        }