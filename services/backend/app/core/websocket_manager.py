from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict, List, Set
import json
import logging
from datetime import datetime
import asyncio

logger = logging.getLogger(__name__)


class WebSocketManager:
    def __init__(self):
        # Active connections: client_id -> websocket
        self.active_connections: Dict[str, WebSocket] = {}

        # Job subscriptions: job_id -> set of client_ids
        self.job_subscriptions: Dict[str, Set[str]] = {}

        # Message history for job (last 100 messages per job)
        self.message_history: Dict[str, List[Dict]] = {}

    async def connect(self, websocket: WebSocket, client_id: str):
        """Accept a new WebSocket connection"""
        await websocket.accept()
        self.active_connections[client_id] = websocket
        logger.info(f"Client {client_id} connected. Total connections: {len(self.active_connections)}")

        # Send connection confirmation
        await self.send_personal_message({
            "type": "connection_established",
            "client_id": client_id,
            "timestamp": datetime.utcnow().isoformat()
        }, client_id)

    def disconnect(self, client_id: str):
        """Remove a client connection"""
        if client_id in self.active_connections:
            del self.active_connections[client_id]
            logger.info(f"Client {client_id} disconnected. Total connections: {len(self.active_connections)}")

        # Remove from all job subscriptions
        for job_id in list(self.job_subscriptions.keys()):
            if client_id in self.job_subscriptions[job_id]:
                self.job_subscriptions[job_id].discard(client_id)
                # Remove job subscription if no clients
                if not self.job_subscriptions[job_id]:
                    del self.job_subscriptions[job_id]

    async def send_personal_message(self, message: Dict, client_id: str):
        """Send message to a specific client"""
        if client_id in self.active_connections:
            try:
                websocket = self.active_connections[client_id]
                message_str = json.dumps(message, default=str)
                await websocket.send_text(message_str)
                logger.debug(f"Message sent to client {client_id}: {message.get('type', 'unknown')}")
            except Exception as e:
                logger.error(f"Failed to send message to client {client_id}: {e}")
                # Remove disconnected client
                self.disconnect(client_id)

    async def broadcast_message(self, message: Dict):
        """Broadcast message to all connected clients"""
        if not self.active_connections:
            return

        message_str = json.dumps(message, default=str)
        disconnected_clients = []

        for client_id, websocket in self.active_connections.items():
            try:
                await websocket.send_text(message_str)
            except Exception as e:
                logger.error(f"Failed to broadcast to client {client_id}: {e}")
                disconnected_clients.append(client_id)

        # Clean up disconnected clients
        for client_id in disconnected_clients:
            self.disconnect(client_id)

    async def subscribe_to_job(self, client_id: str, job_id: str):
        """Subscribe a client to job updates"""
        if job_id not in self.job_subscriptions:
            self.job_subscriptions[job_id] = set()

        self.job_subscriptions[job_id].add(client_id)
        logger.info(f"Client {client_id} subscribed to job {job_id}")

        # Send recent message history for this job
        if job_id in self.message_history:
            for historical_message in self.message_history[job_id][-10:]:  # Last 10 messages
                await self.send_personal_message(historical_message, client_id)

    async def unsubscribe_from_job(self, client_id: str, job_id: str):
        """Unsubscribe a client from job updates"""
        if job_id in self.job_subscriptions and client_id in self.job_subscriptions[job_id]:
            self.job_subscriptions[job_id].discard(client_id)
            logger.info(f"Client {client_id} unsubscribed from job {job_id}")

            # Remove job subscription if no clients
            if not self.job_subscriptions[job_id]:
                del self.job_subscriptions[job_id]

    async def send_job_update(self, job_id: str, message: Dict):
        """Send update to all clients subscribed to a specific job"""
        if job_id not in self.job_subscriptions:
            logger.debug(f"No subscribers for job {job_id}")
            return

        # Add job_id to message if not present
        if "job_id" not in message:
            message["job_id"] = job_id

        # Add timestamp if not present
        if "timestamp" not in message:
            message["timestamp"] = datetime.utcnow().isoformat()

        # Store in message history
        if job_id not in self.message_history:
            self.message_history[job_id] = []

        self.message_history[job_id].append(message.copy())

        # Keep only last 100 messages per job
        if len(self.message_history[job_id]) > 100:
            self.message_history[job_id] = self.message_history[job_id][-100:]

        # Send to all subscribed clients
        subscribers = list(self.job_subscriptions[job_id])  # Create copy to avoid modification during iteration
        disconnected_clients = []

        message_str = json.dumps(message, default=str)

        for client_id in subscribers:
            if client_id in self.active_connections:
                try:
                    await self.active_connections[client_id].send_text(message_str)
                    logger.debug(f"Job update sent to client {client_id} for job {job_id}")
                except Exception as e:
                    logger.error(f"Failed to send job update to client {client_id}: {e}")
                    disconnected_clients.append(client_id)

        # Clean up disconnected clients
        for client_id in disconnected_clients:
            self.disconnect(client_id)

        logger.info(f"Job update sent to {len(subscribers) - len(disconnected_clients)} clients for job {job_id}")

    async def handle_client_message(self, client_id: str, message: str):
        """Handle incoming message from client"""
        try:
            data = json.loads(message)
            message_type = data.get("type", "unknown")

            if message_type == "subscribe_job":
                job_id = data.get("job_id")
                if job_id:
                    await self.subscribe_to_job(client_id, job_id)

            elif message_type == "unsubscribe_job":
                job_id = data.get("job_id")
                if job_id:
                    await self.unsubscribe_from_job(client_id, job_id)

            elif message_type == "ping":
                await self.send_personal_message({
                    "type": "pong",
                    "timestamp": datetime.utcnow().isoformat()
                }, client_id)

            else:
                logger.warning(f"Unknown message type from client {client_id}: {message_type}")

        except json.JSONDecodeError:
            logger.error(f"Invalid JSON message from client {client_id}: {message}")
        except Exception as e:
            logger.error(f"Error handling message from client {client_id}: {e}")

    def get_connection_stats(self) -> Dict:
        """Get current connection statistics"""
        return {
            "total_connections": len(self.active_connections),
            "active_jobs": len(self.job_subscriptions),
            "total_subscriptions": sum(len(subs) for subs in self.job_subscriptions.values()),
            "connected_clients": list(self.active_connections.keys())
        }