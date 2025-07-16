def is_selected(stock_code, combined_data):
    """
    均线交叉策略示例（金叉）
    :param stock_code: 股票代码
    :param combined_data: 包含历史 + 快照的 DataFrame
    :return: bool 是否选中该股票
    """
    if len(combined_data) < 2:
        return False

    combined_data['MA5'] = combined_data['收盘'].rolling(window=5).mean()
    combined_data['MA10'] = combined_data['收盘'].rolling(window=10).mean()

    last_two = combined_data.tail(2)
    if (last_two.iloc[-2]['MA5'] < last_two.iloc[-2]['MA10']) and \
       (last_two.iloc[-1]['MA5'] > last_two.iloc[-1]['MA10']):
        return True

    return False