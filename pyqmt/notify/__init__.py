# coding=utf-8

import datetime
def get_stock_type(stock_code):
    """判断股票ID对应的证券市场
    匹配规则
    ['50', '51', '60', '90', '110'] 为 sh
    ['00', '13', '18', '15', '16', '18', '20', '30', '39', '115'] 为 sz
    ['5', '6'] 开头的为 sh
    ['8', '4', '9'] 开头的为 bj

    Args:
        stock_code: 股票ID, 若以 'sz', 'sh' 开头直接返回对应类型，否则使用内置规则判断
    Returns:
        'SH' or 'SZ' or 'BJ'
    """
    stock_code = str(stock_code).upper()
    if stock_code.startswith(('sh', 'sz', 'bj')):
        return stock_code[:2].upper()
    if stock_code.startswith(('50', '51', '60', '73', '90', '110', '113', '132', '204', '78')):
        return 'SH'
    if stock_code.startswith(('00', '12', '13', '18', '15', '16', '18', '20', '30', '39', '115', '1318')):
        return 'SZ'
    if stock_code.startswith(('5', '6')):
        return 'SH'
    if stock_code.startswith(('8', '4', '9')):
        return 'BJ'
    return 'SZ'

def get_stock_id_hson_helpers(stock_code: str) -> str:
    code = stock_code
    idx = stock_code.find('.')
    if idx > 0:
        code = code[0:idx]

    suffix = get_stock_type(stock_code)
    code = str(code) + suffix
    return code


def get_stock_id_xt(stock_code: str) -> str:
    code = stock_code
    idx = stock_code.find('.')
    if idx > 0:
        code = code[0:idx]

    suffix = get_stock_type(stock_code)
    code = str(code) + suffix
    return code


def get_stock_id_hson(stock_code: str) -> str:
    """
    转换成股票代码id,600136.XSHG->6001361
    """
    """判断股票ID对应的证券市场
    匹配规则
    ['50', '51', '60', '90', '110'] 为 sh
    ['00', '13', '18', '15', '16', '18', '20', '30', '39', '115'] 为 sz
    ['5', '6', '9'] 开头的为 sh， 其余为 sz
    :param stock_code:股票ID, 若以 'sz', 'sh' 开头直接返回对应类型，否则使用内置规则判断
    :return 'sh' or 'sz'"""
    code = stock_code
    idx = stock_code.find('.')
    if idx > 0:
        code = code[0:idx]
    suffix = ".SS" if get_stock_type(stock_code) == 'SH' else ".SZ"
    code = str(code) + suffix
    return code


def get_stock_id_jq(stock_code):
    code = stock_code
    idx = stock_code.find('.')
    if idx > 0:
        code = code[0:idx]
    suffix = ".XSHG" if get_stock_type(stock_code) == 'SH' else ".XSHE"
    code = str(code) + suffix
    return code


def get_high_low_limit(symbol_code: str, preclose_px: float):
    """
    计算涨跌停价格
    """
    if symbol_code.startswith("300") or symbol_code.startswith("688"):
        return round(preclose_px * 1.2, 2), round(preclose_px * 0.8, 2)
    return round(preclose_px * 1.1, 2), round(preclose_px * 0.9, 2)


def open_time_delta(dt: datetime.datetime):
    """计算当前时间距离开盘时间的分钟数
    """
    hour = dt.hour
    minute = dt.minute
    return {
        9: minute-30,
        10: 30+minute,
        11: 90+minute,
        13: 120+minute,
        14: 180+minute,
        15: 240,
    }.get(hour, 0)


