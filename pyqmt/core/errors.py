import datetime
from enum import IntEnum
from typing import Optional, Protocol

import bidict


class WebErrors(IntEnum):
    """错误码"""

    BAD_PARAMS = 400  # 无效参数
    UNAUTHORIZED = 401  # 未授权
    FORBIDDEN = 403  # 禁止访问
    NOT_FOUND = 404  # 资源不存在
    METHOD_NOT_ALLOWED = 405  # 方法不允许
    INTERNAL_SERVER_ERROR = 500  # 内部服务器错误


class TradeErrors(IntEnum):
    """交易处理错误码"""

    NO_ERROR = 0
    ERROR_UNKNOWN = 1
    ERROR_NOT_LOGIN = 2
    ERROR_BAD_PARAMS = 3
    ERROR_XT_ORDER_FAIL = 4
    ERROR_XT_ORDER_TIMEOUT = 5
    ERROR_ALREADY_EXISTS = 6


class BaseTradeError(BaseException):
    """基础错误处理基类"""

    def __init__(self, code: TradeErrors, msg: str, *args):
        self.code = code
        self.msg = msg
        self.args = args

    def __str__(self):
        return f"({self.code.value} | {self.msg % self.args})"


class WebError(BaseTradeError):
    """web API 中的错误处理基类"""

    ...


class TradeError(BaseTradeError):
    """交易处理错误"""

    ...


class XtQuantTradeError(BaseTradeError):
    """XtQuantTradeError"""

    ...


class XtTradeConnectError(BaseTradeError):
    """XtTradeConnectError"""

    ...


class NoDataForMatch(TradeError):
    """回测撮合时缺少数据"""

    def __init__(
        self,
        security: str,
        dt: datetime.date | datetime.datetime,
        with_stack: bool = False,
    ):
        super().__init__(
            TradeErrors.ERROR_BAD_PARAMS,
            f"failed to match %s, no data at %s",
            security,
            dt,
        )


class CashError(TradeError):
    """回测撮合时资金不足"""

    def __init__(
        self, account: str, required: float, available: float, with_stack: bool = False
    ):
        super().__init__(
            TradeErrors.ERROR_BAD_PARAMS,
            f"Account %s: required %s, available %s",
            account,
            required,
            available,
        )


class VolumeNotMeet(TradeError):
    """回测撮合时成交量不满足"""

    def __init__(self, security: str, price: float, with_stack: bool = False):
        super().__init__(
            TradeErrors.ERROR_BAD_PARAMS,
            f"Volume not meet for %s at %s",
            security,
            price,
        )
