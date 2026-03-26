"""Published data sync services.

Only stock sync remains part of the published subject-app surface.
Legacy local xtdata services stay available via their dedicated modules.
"""

from .stock_sync import StockSyncService

__all__ = ["StockSyncService"]
