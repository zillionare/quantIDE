"""核心枚举类型

与主体工程保持一致
"""

from enum import Enum, IntEnum


class BidType(IntEnum):
    """交易中的委托类型枚举类"""

    FIXED = 0  # 指定价格下单
    LATEST = 1  # 最新价
    MARKET = 2  # 市价单
    MINE_FIRST = 3  # 本方最优价
    PEER_FIRST = 4  # 对方最优价
    UNKNOWN = 255  # 未知


class OrderSide(IntEnum):
    """交易中的买卖方向枚举类"""

    BUY = 1
    SELL = -1
    XDXR = 0
    UNKNOWN = 255

    def __str__(self):
        return {
            OrderSide.BUY: "买入",
            OrderSide.SELL: "卖出",
            OrderSide.XDXR: "分红配股",
        }[self]


class OrderStatus(IntEnum):
    """订单状态枚举类"""

    UNREPORTED = 48  # 未报
    WAIT_REPORTING = 49  # 待报
    REPORTED = 50  # 已报
    REPORTED_CANCEL = 51  # 已报待撤
    PARTSUCC_CANCEL = 52  # 部分成交待撤
    PART_CANCEL = 53  # 部分成交，余下待撤
    CANCELED = 54  # 已撤
    PART_SUCC = 55  # 部分成交
    SUCCEEDED = 56  # 已成交
    JUNK = 57  # 无效订单
    UNKNOWN = 255  # 未知


class BrokerKind(Enum):
    """broker 类型"""

    BACKTEST = "bt"
    SIMULATION = "sim"
    QMT = "qmt"
