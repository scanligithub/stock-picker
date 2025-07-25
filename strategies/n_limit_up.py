# strategies/n_limit_up.py

def is_selected(stock_code, combined_data):
    """
    判断某只股票是否满足 N 连板条件（支持 N=1：任意一天涨停）
    :param stock_code: 股票代码
    :param combined_data: 合并后的历史 + 快照数据（DataFrame）
    :return: tuple (bool 是否选中, date 最近一次涨停日期, float 涨幅) 
    """
    from config import N_CONSECUTIVE_DAYS

    if len(combined_data) < N_CONSECUTIVE_DAYS:
        return False, None, None

    daily_records = combined_data.to_dict('records')

    # N == 1 时，只要过去任意一天涨停就算符合
    if N_CONSECUTIVE_DAYS == 1:
        threshold = 19.8 if stock_code.startswith(('30', '68')) else 9.9
        for day in reversed(daily_records):  # 从最新到最旧遍历
            pct_change = day.get('涨跌幅', -1)
            date_str = day['日期'].strftime('%Y-%m-%d') if '日期' in day else '未知'
            if pct_change >= threshold:
                print(f"[HIT] {stock_code} 在 {date_str} 涨幅 {pct_change:.2f}% ≥ {threshold:.1f}%，[OK] 符合条件")
                return True, day['日期'], pct_change
        return False, None, None

    # N > 1 的情况保持不变
    for i in range(len(daily_records) - (N_CONSECUTIVE_DAYS - 1)):
        window = daily_records[i:i + N_CONSECUTIVE_DAYS]
        is_consecutive_zt = all(
            day.get('涨跌幅', -1) >= (19.8 if stock_code.startswith(('30','68')) else 9.9)
            for day in window
        )
        if is_consecutive_zt:
            last_day = window[-1]
            last_date = last_day['日期'].strftime('%Y-%m-%d')
            print(f"🎯 {stock_code} 在 {last_date} 及之前连续 {N_CONSECUTIVE_DAYS} 日涨停，✅ 符合条件")
            return True, last_day['日期'], last_day.get('涨跌幅', None)

    return False, None, None