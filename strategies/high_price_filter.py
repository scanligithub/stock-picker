# strategies/high_price_filter.py

import pandas as pd

def is_selected(stock_code, combined_data):
    """
    筛选今日股价大于 100 元的股票
    :param stock_code: 股票代码
    :param combined_data: 历史 + 快照合并数据（DataFrame）
    :return: bool 是否满足条件
    """

    if combined_data.empty:
        print(f"{stock_code} 数据为空，跳过")
        return False

    # 获取最新的一条记录（通常是今天）
    latest_data = combined_data.iloc[-1]

    # 检查 '收盘' 字段是否存在于 latest_data 中
    if '收盘' not in latest_data:
        print(f"⚠️ {stock_code} 缺少 '收盘' 字段")
        return False

    close_price = latest_data['收盘']

    # 检查是否是数字类型
    if not isinstance(close_price, (int, float)) and not (isinstance(close_price, str) and close_price.replace('.', '', 1).isdigit()):
        print(f"❌ {stock_code} 收盘价无效：{close_price}")
        return False

    try:
        close_price = float(close_price)
    except Exception as e:
        print(f"❌ {stock_code} 类型转换失败: {e}")
        return False

    # 📢 打印当前股价
    print(f"📊 {stock_code} 当前股价为 {close_price:.2f} 元")

    if close_price > 100:
        print(f"🎯 {stock_code} 当前股价为 {close_price:.2f} 元，✅ 符合条件")
        return True

    return False