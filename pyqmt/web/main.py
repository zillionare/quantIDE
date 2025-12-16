import os
import sys
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
import cfg4py

from pyqmt.web.apis.login import router as login_router
from pyqmt.web.apis.dashboard import router as dashboard_router

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variable for configuration
cfg = None

def get_config_dir():
    """Get the configuration directory based on environment variable PYQMT_CONFIG_DIR.
    
    If not set, defaults to pyqmt/config under the project root.
    """
    config_dir = os.environ.get("PYQMT_CONFIG_DIR")
    if config_dir:
        return config_dir
    
    # Default to pyqmt/config under project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(project_root, "config")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup and shutdown events."""
    global cfg
    
    # Startup
    logger.info("Starting pyqmt web service...")
    
    # Initialize configuration
    cfg = cfg4py.init(get_config_dir())
    logger.info(f"Configuration loaded from {get_config_dir()}")
    
    yield  # App is running
    
    # Shutdown
    logger.info("Shutting down pyqmt web service...")



def create_app():
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="PyQMT Web Service",
        description="Web interface for PyQMT trading system",
        version="0.1.0",
        lifespan=lifespan
    )
    
    # Mount static files
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    if os.path.exists(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")
        logger.info(f"Static files mounted from {static_dir}")
    
    # Include routers
    app.include_router(login_router)
    app.include_router(dashboard_router)
    
    logger.info("Application created and configured")
    return app

# Create the FastAPI app instance
app = create_app()
