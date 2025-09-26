from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging
import uuid

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

# Global WebSocket manager
websocket_manager = WebSocketManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    # Startup
    logger.info("Starting BigData Processing API...")

    try:
        # Create database tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Failed to create database tables: {e}")

    # Add websocket manager to app state
    app.state.websocket_manager = websocket_manager

    logger.info("Application startup completed")
    yield

    # Shutdown
    logger.info("Shutting down BigData Processing API...")


# Create FastAPI application
app = FastAPI(
    title=settings.PROJECT_NAME,
    description="API for processing large files with multiple destinations",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
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

        # Send notification to subscribed clients
        await websocket_manager.send_job_update(job_id, {
            "type": "job_update",
            "job_id": job_id,
            "data": notification
        })

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


# Startup message
@app.on_event("startup")
async def startup_message():
    logger.info(f"🚀 {settings.PROJECT_NAME} is running!")
    logger.info(f"📖 API Documentation: http://localhost:8000/docs")
    logger.info(f"🔌 WebSocket URL: ws://localhost:8000/ws/{{client_id}}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )