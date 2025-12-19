# Web pages package initialization

# Import page modules to make them easily accessible
from . import login
from .home import index as dashboard_routes

__all__ = ['login', 'dashboard_routes']
