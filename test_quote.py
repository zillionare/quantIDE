"""测试 subscribe_whole_quote 是否能接收指数行情"""

import sys
import time

# 添加 xtquant 路径
sys.path.insert(0, r"C:\apps\xtquant")

try:
    import xtquant.xtdata as xt
    print("✓ xtquant 导入成功")
except ImportError as e:
    print(f"✗ xtquant 导入失败: {e}")
    sys.exit(1)

# 存储接收到的行情数据
received_data = {
    "stocks": [],  # 个股
    "indices": [],  # 指数
}

# 定义指数代码列表
INDEX_CODES = [
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深成指
    "000300.SH",  # 沪深300
    "000905.SH",  # 中证500
    "000852.SH",  # 中证1000
    "000688.SH",  # 科创50
]

def on_tick(data):
    """行情回调函数"""
    for code, item in data.items():
        # 判断是否为指数（简单判断：代码以000/399开头，或者名称包含"指数"）
        is_index = False
        if any(code.startswith(idx.split(".")[0]) for idx in INDEX_CODES):
            is_index = True
        
        if is_index:
            if code not in [i["code"] for i in received_data["indices"]]:
                received_data["indices"].append({
                    "code": code,
                    "name": item.get("name", "Unknown"),
                    "price": item.get("lastPrice", 0),
                })
                print(f"[指数] {code}: {item.get('lastPrice', 0)}")
        else:
            if code not in [s["code"] for s in received_data["stocks"]]:
                received_data["stocks"].append({
                    "code": code,
                    "name": item.get("name", "Unknown"),
                    "price": item.get("lastPrice", 0),
                })
                print(f"[个股] {code}: {item.get('lastPrice', 0)}")

def test_subscribe_whole_quote():
    """测试全推订阅"""
    print("\n=== 测试 subscribe_whole_quote ===")
    print("订阅市场: ['SH', 'SZ']")
    
    # 订阅全市场
    xt.subscribe_whole_quote(["SH", "SZ"], on_tick)
    print("✓ 订阅成功，等待接收数据...")
    
    # 等待接收数据
    for i in range(30):  # 等待30秒
        time.sleep(1)
        if i % 5 == 0:
            print(f"  已等待 {i} 秒，接收到 {len(received_data['stocks'])} 只个股，{len(received_data['indices'])} 个指数")
    
    print("\n=== 测试结果 ===")
    print(f"个股数量: {len(received_data['stocks'])}")
    print(f"指数数量: {len(received_data['indices'])}")
    
    if received_data["indices"]:
        print("\n接收到的指数列表:")
        for idx in received_data["indices"]:
            print(f"  - {idx['code']}: {idx['name']} = {idx['price']}")
    else:
        print("\n未接收到任何指数数据")
    
    return len(received_data["indices"]) > 0

if __name__ == "__main__":
    has_indices = test_subscribe_whole_quote()
    
    if has_indices:
        print("\n✓ 结论: subscribe_whole_quote(['SH', 'SZ']) 可以接收到指数行情")
    else:
        print("\n✗ 结论: subscribe_whole_quote(['SH', 'SZ']) 不能接收到指数行情")
        print("  需要单独订阅指数代码")
