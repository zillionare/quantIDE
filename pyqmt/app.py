"""Main application entry point."""

from pyqmt.web.main import create_app

# Create the FastAPI application
app = create_app()

if __name__ == "__main__":
    import uvicorn
    import os
    
    uvicorn.run(
        "pyqmt.app:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000)),
        reload=os.environ.get("PYQMT_RELOAD", "false").lower() == "true"
    )
