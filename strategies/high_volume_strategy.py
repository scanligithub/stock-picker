def is_selected(stock_code, combined_data):
    """
    判断股票是否满足高成交量策略条件：
    1. 最近10个交易日内有一个交易日的成交量是20日成交量均线值的4倍以上
    2. 最近10个交易日内涨停次数不能大于2（涨跌幅>=9.9%）
    3. 最近10个交易日内，有4个交易日连续缩量下跌（成交量比前一天少，且收盘价比前一天低）
    
    :param stock_code: 股票代码
    :param combined_data: 包含历史 + 快照的 DataFrame
    :return: tuple (是否选中该股票, 当天成交量, 上一交易日成交量, 触发日期, 涨跌幅)
    """
    # 检查数据完整性
    if len(combined_data) < 30:  # 需要至少30天数据来计算20日均线
        return False
    
    # 提取必要数据
    df = combined_data.copy()
    
    # 检查是否有成交量数据
    if '成交量' not in df.columns:
        return False
    
    # 计算20日成交量均线
    df['MA20_Volume'] = df['成交量'].rolling(window=20).mean()
    
    # 检查最近10个交易日（从现在往回溯1到10个交易日）
    recent_days = df.tail(10)
    
    # 确保最近10天的20日均线数据有效
    if recent_days['MA20_Volume'].isna().any():
        return False
    
    # 检查是否有交易日的成交量是20日成交量均线值的3倍以上
    high_volume_condition = (recent_days['成交量'] >= 4 * recent_days['MA20_Volume'])
    
    if not high_volume_condition.any():
        return False
    
    # 新增条件：最近10个交易日内涨停次数不能大于2
    # 假设涨停为涨跌幅大于等于9.9%
    limit_up_condition = recent_days['涨跌幅'] >= 9.9
    limit_up_count = limit_up_condition.sum()
    
    if limit_up_count > 2:
        return False
    
    # 新增条件：最近10个交易日内，有4个交易日连续缩量下跌
    # 缩量下跌：成交量比前一天少，且收盘价比前一天低
    recent_days_sorted = recent_days.sort_values('日期', ascending=True)  # 按日期从远到近排序
    
    # 计算每日是否为缩量下跌
    volume_decrease = recent_days_sorted['成交量'].diff() < 0  # 成交量比前一天少
    price_decrease = recent_days_sorted['收盘'].diff() < 0      # 收盘价比前一天低
    
    # 同时满足成交量减少和价格下跌的条件
    shrink_decline = volume_decrease & price_decrease  # 成交量减少且价格下跌
    
    # 检查是否存在连续4天的缩量下跌
    consecutive_count = 0
    max_consecutive = 0
    
    for is_shrink_decline in shrink_decline:
        if is_shrink_decline:
            consecutive_count += 1
            max_consecutive = max(max_consecutive, consecutive_count)
        else:
            consecutive_count = 0
    
    if max_consecutive < 4:
        return False
    
    # 获取满足条件的日期
    trigger_date = recent_days[high_volume_condition].iloc[-1]['日期']
    
    # 获取当天和上一交易日的成交量
    today_volume = df['成交量'].iloc[-1]
    yesterday_volume = df['成交量'].iloc[-2] if len(df) >= 2 else None
    
    # 确保返回的是Python原生int类型，而不是numpy类型
    if today_volume is not None:
        today_volume = int(today_volume)
    if yesterday_volume is not None:
        yesterday_volume = int(yesterday_volume)
    
    # 获取涨跌幅
    change_percent = df['涨跌幅'].iloc[-1] if '涨跌幅' in df.columns else None
    
    return True, today_volume, yesterday_volume, trigger_date, change_percent