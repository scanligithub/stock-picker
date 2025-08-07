def is_selected(stock_code, combined_data):
    """
    周K线多头排列策略
    1、MA5、MA10、MA20、MA30均为周K线图的移动平均线；
    2、本交易周MA5 大于等于 MA10，并且MA10 大于等于 MA20，并且MA20大于等于MA30；
    3、上一个交易周没有达到2所述的条件。

    :param stock_code: 股票代码
    :param combined_data: 包含历史 + 快照的 DataFrame
    :return: tuple (是否选中该股票, 当天成交量, 上一交易日成交量, 触发日期, 涨跌幅)
    """
    # 检查数据完整性
    if len(combined_data) < 300:  # 需要至少300天数据来计算周线MA30
        return False

    # 提取必要数据
    df = combined_data.copy()
    
    # 检查是否有收盘价数据
    if '收盘' not in df.columns:
        return False
    
    # 计算周线数据
    # 将日期转换为周标识（周一为一周开始）
    df['周标识'] = df['日期'].dt.to_period('W')
    
    # 按周聚合数据，取每周最后一个交易日的收盘价
    weekly_df = df.groupby('周标识').agg({
        '收盘': 'last'
    }).reset_index()
    
    # 重新索引
    weekly_df['日期'] = weekly_df['周标识'].dt.start_time
    weekly_df = weekly_df.sort_values('日期').reset_index(drop=True)
    
    # 计算周线移动平均线
    weekly_df['MA5'] = weekly_df['收盘'].rolling(window=5).mean()
    weekly_df['MA10'] = weekly_df['收盘'].rolling(window=10).mean()
    weekly_df['MA20'] = weekly_df['收盘'].rolling(window=20).mean()
    weekly_df['MA30'] = weekly_df['收盘'].rolling(window=30).mean()
    
    # 确保至少有两周的数据
    if len(weekly_df) < 2:
        return False
    
    # 检查本周是否满足多头排列条件
    current_week = weekly_df.iloc[-1]
    
    # 检查本周是否满足MA5 >= MA10 >= MA20 >= MA30
    if not (current_week['MA5'] >= current_week['MA10'] >= current_week['MA20'] >= current_week['MA30']):
        return False
    
    # 检查上周是否不满足多头排列条件
    if len(weekly_df) >= 2:
        last_week = weekly_df.iloc[-2]
        # 如果上周满足多头排列条件，则不选择
        if (last_week['MA5'] >= last_week['MA10'] >= last_week['MA20'] >= last_week['MA30']):
            return False
    
    # 获取当天和上一交易日的成交量
    today_volume = df['成交量'].iloc[-1] if '成交量' in df.columns else None
    yesterday_volume = df['成交量'].iloc[-2] if '成交量' in df.columns and len(df) >= 2 else None
    
    # 确保返回的是Python原生int类型，而不是numpy类型
    if today_volume is not None:
        today_volume = int(today_volume)
    if yesterday_volume is not None:
        yesterday_volume = int(yesterday_volume)
    
    # 获取涨跌幅
    change_percent = df['涨跌幅'].iloc[-1] if '涨跌幅' in df.columns else None
    
    # 返回结果
    return True, today_volume, yesterday_volume, current_week['日期'], change_percent