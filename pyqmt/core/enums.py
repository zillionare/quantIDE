from enum import IntEnum


class BidType(IntEnum):
    FIXED  = 0              # 指定价格下单
    MARKET = 1              # 最新价（市价）下单
    UNKNOWN = 255           # 未知

class OrderSide(IntEnum):
    BUY = 1
    SELL = -1
    XDXR = 0
    UNKNOWN = 255               # 未知

    def __str__(self):
        return {
            OrderSide.BUY: "买入",
            OrderSide.SELL: "卖出",
            OrderSide.XDXR: "分红配股",
        }[self]
    
class OrderStatus(IntEnum):
    UNREPORTED = 48             # 未报
    WAIT_REPORTING = 49         # 待报
    REPORTED = 50               # 已报
    REPORTED_CANCEL = 51        # 已报待撤
    PARTSUCC_CANCEL = 52        # 部分成交待撤
    PART_CANCEL = 53            # 部分成交，余下待撤
    CANCELED = 54               # 已撤
    PART_SUCC = 55              # 部分成交
    SUCCEEDED = 56              # 已成交
    JUNK = 57                   # 无效订单
    UNKNOWN = 255               # 未知
