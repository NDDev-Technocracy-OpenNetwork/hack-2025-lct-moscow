from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging
import uuid
import json
import socketio

from app.api.v1 import api_router
from app.core.websocket_manager import WebSocketManager
from app.database import engine
from app.models import Base
from app.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global managers
websocket_manager = WebSocketManager()

# Socket.IO server setup
sio = socketio.AsyncServer(
    async_mode='asgi',
    cors_allowed_origins='*',
    logger=True,
    engineio_logger=True
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    # Startup
    logger.info("Application startup initiated")

    try:
        # Create database tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables initialized")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")

    # Initialize application state
    app.state.websocket_manager = websocket_manager
    app.state.sio = sio

    # Link Socket.IO to WebSocketManager
    websocket_manager.sio = sio

    # Start Redis subscriber for real-time progress updates
    try:
        await websocket_manager.start_redis_subscriber()
        logger.info("Redis subscriber started for real-time progress updates")
    except Exception as e:
        logger.error(f"Failed to start Redis subscriber: {e}")

    logger.info("Application startup completed")
    yield

    # Shutdown
    logger.info("Application shutdown initiated")

    # Stop Redis subscriber
    try:
        await websocket_manager.stop_redis_subscriber()
        logger.info("Redis subscriber stopped")
    except Exception as e:
        logger.error(f"Failed to stop Redis subscriber: {e}")


# Initialize FastAPI application
app = FastAPI(
    title=settings.PROJECT_NAME,
    description="REST API for distributed file processing with configurable destinations",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(api_router, prefix=settings.API_V1_STR)

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "bigdata-backend",
        "version": "1.0.0"
    }


# WebSocket endpoint for real-time updates
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    """WebSocket endpoint for real-time communication"""

    await websocket_manager.connect(websocket, client_id)
    logger.info(f"Client {client_id} connected via WebSocket")

    try:
        while True:
            # Receive message from client
            message = await websocket.receive_text()
            logger.debug(f"Received message from {client_id}: {message}")

            # Handle the message
            await websocket_manager.handle_client_message(client_id, message)

    except WebSocketDisconnect:
        websocket_manager.disconnect(client_id)
        logger.info(f"Client {client_id} disconnected from WebSocket")
    except Exception as e:
        logger.error(f"WebSocket error for client {client_id}: {e}")
        websocket_manager.disconnect(client_id)


# WebSocket status endpoint
@app.get("/ws/status")
async def websocket_status():
    """Get WebSocket connection status"""
    return websocket_manager.get_connection_stats()


# Notification endpoint for external services (Airflow callbacks)
@app.post("/api/v1/notify/{job_id}")
async def notify_job_update(job_id: str, notification: dict):
    """Endpoint for receiving job updates from external services"""

    try:
        # Validate job_id format
        uuid.UUID(job_id)

        # Send notification to subscribed clients via both WebSocket and Socket.IO
        notification_data = {
            "type": "job_update",
            "job_id": job_id,
            "data": notification
        }

        await websocket_manager.send_job_update(job_id, notification_data)

        logger.info(f"Notification sent for job {job_id}: {notification.get('status', 'unknown')}")

        return {"status": "notification sent", "job_id": job_id}

    except ValueError:
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid job ID format"}
        )
    except Exception as e:
        logger.error(f"Error sending notification for job {job_id}: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Failed to send notification"}
        )


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""

    logger.error(f"Unhandled exception: {exc}", exc_info=True)

    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": "An unexpected error occurred"
        }
    )


# Startup logging
@app.on_event("startup")
async def startup_message():
    logger.info(f"{settings.PROJECT_NAME} started successfully")
    logger.info("API Documentation available at /docs")
    logger.info("WebSocket endpoint available at /ws/{{client_id}}")


# Socket.IO event handlers
@sio.event
async def connect(sid, environ):
    """Handle Socket.IO client connection"""
    logger.info(f"Socket.IO client {sid} connected (total: {len(websocket_manager.job_subscribers) + 1})")
    # Don't add to active_connections for Socket.IO - it's managed by Socket.IO itself
    return True

@sio.event
async def disconnect(sid):
    """Handle Socket.IO client disconnection"""
    logger.info(f"Socket.IO client {sid} disconnected")
    # Clean up subscriptions
    if sid in websocket_manager.client_subscriptions:
        job_ids = list(websocket_manager.client_subscriptions[sid])
        for job_id in job_ids:
            if job_id in websocket_manager.job_subscribers:
                websocket_manager.job_subscribers[job_id].discard(sid)
        del websocket_manager.client_subscriptions[sid]

    # Clean up any remaining job_subscribers entries
    for job_id in list(websocket_manager.job_subscribers.keys()):
        if sid in websocket_manager.job_subscribers[job_id]:
            websocket_manager.job_subscribers[job_id].discard(sid)
        if not websocket_manager.job_subscribers[job_id]:
            del websocket_manager.job_subscribers[job_id]

@sio.event
async def subscribe_job(sid, data):
    """Handle job subscription"""
    job_id = data.get("job_id")
    if job_id:
        await websocket_manager.subscribe_to_job(sid, job_id)
        logger.info(f"Socket.IO client {sid} subscribed to job {job_id}")
        await sio.emit('subscribed', {"job_id": job_id, "status": "subscribed"}, room=sid)

@sio.event
async def unsubscribe_job(sid, data):
    """Handle job unsubscription"""
    job_id = data.get("job_id")
    if job_id:
        await websocket_manager.unsubscribe_from_job(sid, job_id)
        logger.info(f"Socket.IO client {sid} unsubscribed from job {job_id}")
        await sio.emit('unsubscribed', {"job_id": job_id, "status": "unsubscribed"}, room=sid)

# Mount Socket.IO app
socket_app = socketio.ASGIApp(sio, app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )