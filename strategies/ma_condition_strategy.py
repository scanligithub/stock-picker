def is_selected(stock_code, combined_data):
    """
    自定义均线条件策略
    1、60日均线向上；
    2、30日均价线上穿60日均价线，且发生在最近5个交易日内；
    3、今日收盘价大于MA5；
    4: 今日成交量大于上一交易日成交量;

    :param stock_code: 股票代码
    :param combined_data: 包含历史 + 快照的 DataFrame
    :return: tuple (是否选中该股票, 当天成交量, 上一交易日成交量)
    """
    # 检查数据完整性
    if len(combined_data) < 60:
        return False

    # 提取必要数据
    df = combined_data.copy()
    df['MA5'] = df['收盘'].rolling(window=5).mean()
    df['MA30'] = df['收盘'].rolling(window=30).mean()
    df['MA60'] = df['收盘'].rolling(window=60).mean()

    # 确保均线数据有效
    if df['MA60'].isna().iloc[-1] or df['MA30'].isna().iloc[-1] or df['MA5'].isna().iloc[-1]:
        return False

    # 条件1: 60日均线向上
    if df['MA60'].iloc[-1] <= df['MA60'].iloc[-2]:
        return False

    # 条件2: 30日均价线上穿60日均价线，且发生在最近5个交易日内
    # 检查最近5天内是否有上穿信号
    crossover_detected = False
    for i in range(1, min(6, len(df))):  # 检查最近5天
        if df['MA30'].iloc[-i] > df['MA60'].iloc[-i] and df['MA30'].iloc[-i-1] <= df['MA60'].iloc[-i-1]:
            crossover_detected = True
            break
    
    if not crossover_detected:
        return False

    # 条件3: 今日收盘价大于MA5
    if df['收盘'].iloc[-1] <= df['MA5'].iloc[-1]:
        return False

    # 条件4: 今日成交量大于上一交易日成交量
    today_volume = df['成交量'].iloc[-1] if '成交量' in df.columns else None
    yesterday_volume = df['成交量'].iloc[-2] if '成交量' in df.columns and len(df) >= 2 else None
    if today_volume is None or yesterday_volume is None or today_volume <= yesterday_volume:
        return False

    # 获取当天和上一交易日的成交量
    today_volume = df['成交量'].iloc[-1] if '成交量' in df.columns else None
    yesterday_volume = df['成交量'].iloc[-2] if '成交量' in df.columns and len(df) >= 2 else None
    
    # 调试信息
    print(f"[DEBUG] {stock_code} 成交量数据: {df['成交量'].tail(3).tolist()}")
    
    # 确保返回的是Python原生int类型，而不是numpy类型
    if today_volume is not None:
        today_volume = int(today_volume)
    if yesterday_volume is not None:
        yesterday_volume = int(yesterday_volume)
    
    # 获取涨跌幅
    change_percent = df['涨跌幅'].iloc[-1] if '涨跌幅' in df.columns else None
    
    return True, today_volume, yesterday_volume, None, change_percent