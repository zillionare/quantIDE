"""页面模块

提供 Web 页面组件。
"""

from qmt_gateway.web.pages.data_mgmt import DataMgmtPage
from qmt_gateway.web.pages.init_wizard import InitWizardPage
from qmt_gateway.web.pages.trading import TradingPage

__all__ = ["InitWizardPage", "TradingPage", "DataMgmtPage"]
