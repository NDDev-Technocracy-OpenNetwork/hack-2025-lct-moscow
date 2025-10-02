from fastapi import APIRouter
from . import jobs, files, destinations, schema

api_router = APIRouter()
api_router.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
api_router.include_router(files.router, prefix="/files", tags=["files"])
api_router.include_router(destinations.router, prefix="/destinations", tags=["destinations"])
api_router.include_router(schema.router, prefix="/schema", tags=["schema"])

__all__ = ["api_router"]