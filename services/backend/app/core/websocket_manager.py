from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict, List, Set, Optional
import json
import logging
from datetime import datetime
import asyncio
import redis.asyncio as aioredis
import os

logger = logging.getLogger(__name__)


class WebSocketManager:
    def __init__(self):
        # Active connections: client_id -> websocket
        self.active_connections: Dict[str, WebSocket] = {}

        # Job subscriptions: job_id -> set of client_ids
        self.job_subscriptions: Dict[str, Set[str]] = {}

        # Client subscriptions (for Socket.IO): client_id -> set of job_ids
        self.client_subscriptions: Dict[str, Set[str]] = {}

        # Job subscribers (for Socket.IO): job_id -> set of client_ids
        self.job_subscribers: Dict[str, Set[str]] = {}

        # Message history for job (last 100 messages per job)
        self.message_history: Dict[str, List[Dict]] = {}

        # Socket.IO server reference (set from main.py)
        self.sio = None

        # Redis client for PubSub
        self.redis_client: Optional[aioredis.Redis] = None
        self.redis_pubsub = None
        self.subscriber_task: Optional[asyncio.Task] = None

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

        # Also update job_subscribers for Socket.IO
        if job_id not in self.job_subscribers:
            self.job_subscribers[job_id] = set()
        self.job_subscribers[job_id].add(client_id)

        # Track client subscriptions
        if client_id not in self.client_subscriptions:
            self.client_subscriptions[client_id] = set()
        self.client_subscriptions[client_id].add(job_id)

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
        # Add job_id to message if not present
        if "job_id" not in message:
            message["job_id"] = job_id

        # Add timestamp if not present
        if "timestamp" not in message:
            message["timestamp"] = datetime.utcnow().isoformat()

        # Store in message history (always store, even if no subscribers)
        if job_id not in self.message_history:
            self.message_history[job_id] = []

        self.message_history[job_id].append(message.copy())

        # Keep only last 100 messages per job
        if len(self.message_history[job_id]) > 100:
            self.message_history[job_id] = self.message_history[job_id][-100:]

        # Send via Socket.IO if available
        if self.sio and job_id in self.job_subscribers and self.job_subscribers[job_id]:
            subscribers = list(self.job_subscribers[job_id])
            for client_id in subscribers:
                try:
                    await self.sio.emit('progress_update', message, room=client_id)
                    logger.info(f"Socket.IO progress update sent to {client_id} for job {job_id}")
                except Exception as e:
                    logger.error(f"Failed to send Socket.IO update to {client_id}: {e}")
            return

        # Check if there are subscribers
        if job_id not in self.job_subscriptions or not self.job_subscriptions[job_id]:
            logger.info(f"Job update stored for {job_id} (no active subscribers)")
            return

        # Send to all subscribed clients via WebSocket
        subscribers = list(self.job_subscriptions[job_id])  # Create copy to avoid modification during iteration
        disconnected_clients = []

        message_str = json.dumps(message, default=str)

        for client_id in subscribers:
            if client_id in self.active_connections:
                try:
                    await self.active_connections[client_id].send_text(message_str)
                    logger.info(f"Job update sent to client {client_id} for job {job_id}")
                except Exception as e:
                    logger.error(f"Failed to send job update to client {client_id}: {e}")
                    disconnected_clients.append(client_id)

        # Clean up disconnected clients
        for client_id in disconnected_clients:
            self.disconnect(client_id)

        logger.info(f"Job update sent to {len(subscribers) - len(disconnected_clients)} clients for job {job_id}")

    async def publish_progress(self, job_id: str, progress_data: Dict):
        """Publish progress update for a job (alias for send_job_update)"""
        return await self.send_job_update(job_id, progress_data)

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

    async def start_redis_subscriber(self):
        """Initialize Redis connection and start subscriber task"""
        try:
            redis_url = os.getenv('REDIS_URL', 'redis://redis:6379/0')
            self.redis_client = aioredis.from_url(redis_url, decode_responses=True)

            # Test connection
            await self.redis_client.ping()
            logger.info(f"Redis connection established: {redis_url}")

            # Start subscriber background task
            self.subscriber_task = asyncio.create_task(self._redis_subscriber_loop())
            logger.info("Redis subscriber task started")

        except Exception as e:
            logger.error(f"Failed to initialize Redis subscriber: {e}", exc_info=True)
            raise

    async def stop_redis_subscriber(self):
        """Stop Redis subscriber and cleanup"""
        try:
            if self.subscriber_task and not self.subscriber_task.done():
                self.subscriber_task.cancel()
                try:
                    await self.subscriber_task
                except asyncio.CancelledError:
                    pass
                logger.info("Redis subscriber task stopped")

            if self.redis_pubsub:
                await self.redis_pubsub.unsubscribe()
                await self.redis_pubsub.close()
                logger.info("Redis PubSub unsubscribed and closed")

            if self.redis_client:
                await self.redis_client.close()
                logger.info("Redis client closed")

        except Exception as e:
            logger.error(f"Error stopping Redis subscriber: {e}", exc_info=True)

    async def _redis_subscriber_loop(self):
        """Background task that listens to Redis PubSub and forwards messages to Socket.IO clients"""
        try:
            self.redis_pubsub = self.redis_client.pubsub()

            # Subscribe to all job progress channels
            await self.redis_pubsub.psubscribe('job_progress:*')
            logger.info("Subscribed to Redis channel pattern: job_progress:*")

            async for message in self.redis_pubsub.listen():
                if message['type'] == 'pmessage':
                    try:
                        # Parse the message
                        channel = message['channel']
                        data = json.loads(message['data'])

                        job_id = data.get('job_id')
                        if not job_id:
                            logger.warning(f"Received message without job_id: {data}")
                            continue

                        logger.info(f"Received progress update from Redis for job {job_id}")

                        # Store in message history
                        if job_id not in self.message_history:
                            self.message_history[job_id] = []

                        self.message_history[job_id].append(data.copy())

                        # Keep only last 100 messages per job
                        if len(self.message_history[job_id]) > 100:
                            self.message_history[job_id] = self.message_history[job_id][-100:]

                        # Forward to Socket.IO clients
                        if self.sio and job_id in self.job_subscribers and self.job_subscribers[job_id]:
                            subscribers = list(self.job_subscribers[job_id])
                            for client_id in subscribers:
                                try:
                                    await self.sio.emit('progress_update', data, room=client_id)
                                    logger.info(f"Forwarded progress update to Socket.IO client {client_id} for job {job_id}")
                                except Exception as e:
                                    logger.error(f"Failed to forward update to Socket.IO client {client_id}: {e}")
                        else:
                            logger.info(f"Progress update stored for job {job_id} (no active Socket.IO subscribers)")

                        # Forward to WebSocket clients
                        if job_id in self.job_subscriptions and self.job_subscriptions[job_id]:
                            message_str = json.dumps(data, default=str)
                            subscribers = list(self.job_subscriptions[job_id])

                            for client_id in subscribers:
                                if client_id in self.active_connections:
                                    try:
                                        await self.active_connections[client_id].send_text(message_str)
                                        logger.info(f"Forwarded progress update to WebSocket client {client_id} for job {job_id}")
                                    except Exception as e:
                                        logger.error(f"Failed to forward update to WebSocket client {client_id}: {e}")

                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse Redis message: {e}")
                    except Exception as e:
                        logger.error(f"Error processing Redis message: {e}", exc_info=True)

        except asyncio.CancelledError:
            logger.info("Redis subscriber loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Redis subscriber loop error: {e}", exc_info=True)
            # Try to restart after error
            await asyncio.sleep(5)
            if self.redis_client:
                logger.info("Attempting to restart Redis subscriber...")
                self.subscriber_task = asyncio.create_task(self._redis_subscriber_loop())


# Global WebSocket manager instance
ws_manager = WebSocketManager()