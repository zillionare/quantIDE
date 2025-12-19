# Web pages package initialization

# Import page modules to make them easily accessible
from . import login
from .home import index as dashboard_routes
from .backtest import index as backtest_routes
from .backtest_results import index as backtest_results_routes

__all__ = ['login', 'dashboard_routes', 'backtest_routes', 'backtest_results_routes']
