import pandas as pd
import numpy as np
from utils.data_loader import load_clean_hist_data, get_clean_snapshot_data
from strategies.high_volume_strategy import is_selected

# 加载历史数据
try:
    hist_data = load_clean_hist_data()
    print(f"历史数据加载成功，共 {len(hist_data)} 条记录")
except Exception as e:
    print(f"历史数据加载失败: {e}")
    exit(1)

# 获取实时快照数据
try:
    snapshot_data = get_clean_snapshot_data()
    if snapshot_data is not None:
        print(f"快照数据加载成功，共 {len(snapshot_data)} 条记录")
    else:
        print("快照数据加载失败")
        exit(1)
except Exception as e:
    print(f"快照数据加载异常: {e}")
    exit(1)

# 合并数据
try:
    combined_data = pd.concat([hist_data, snapshot_data], ignore_index=True)
    combined_data['日期'] = pd.to_datetime(combined_data['日期'])
    combined_data = combined_data.sort_values(['代码', '日期']).reset_index(drop=True)
    print("数据合并完成")
except Exception as e:
    print(f"数据合并失败: {e}")
    exit(1)

# 选择特定股票进行测试
# 南京新百 600682.SH
# 读者传媒 603999.SH
# 浙江建投 002761.SZ
# 中通客车 000957.SZ
# 深振业A 000006.SZ
# 湖南发展 000722.SZ
# 顺控发展 003039.SZ
# 焦点科技 002315.SZ
# 河化股份 000953.SZ
# 国光连锁 603916.SH
# 华媒控股 000607.SZ
# 通程控股 000419.SZ
# 人人乐 002336.SZ
# 浙江东日 600113.SH
# 长江通信 600345.SH
# 中国卫通 601698.SH
# 贵广网络 600996.SH
# 人民网 603000.SH
# 东方集团 600811.SH
# 宁波中百 600857.SH
# 恒银科技 603106.SH
# 天娱数科 002354.SZ
# 荣联科技 002642.SZ
# 二六三 002467.SZ
# 省广集团 002400.SZ
# 海峡创新 300300.SZ
# 蓝色光标 300058.SZ
# 三六五网 300295.SZ
# 焦点科技 002315.SZ
# 拓维信息 002261.SZ
# 恒信东方 300081.SZ
# 初灵信息 300250.SZ
# 丝路视觉 300556.SZ
# 奥飞娱乐 002292.SZ
# 三六零 601360.SH
# 人民网 603000.SH
# 浙江东日 600113.SH
# 中国卫通 601698.SH
# 贵广网络 600996.SH
# 长江通信 600345.SH
# 东方集团 600811.SH
# 宁波中百 600857.SH
# 恒银科技 603106.SH
# 天娱数科 002354.SZ
# 荣联科技 002642.SZ
# 二六三 002467.SZ
# 省广集团 002400.SZ
# 海峡创新 300300.SZ
# 蓝色光标 300058.SZ
# 三六五网 300295.SZ
# 焦点科技 002315.SZ
# 拓维信息 002261.SZ
# 恒信东方 300081.SZ
# 初灵信息 300250.SZ
# 丝路视觉 300556.SZ
# 奥飞娱乐 002292.SZ
# 三六零 601360.SH

target_stock = '000802.SZ'  # 北京文化

# 查找目标股票数据
stock_data = combined_data[combined_data['代码'] == target_stock].copy()

if not stock_data.empty:
    print(f"股票 {target_stock} 数据:")
    print(stock_data[['日期', '成交量', '涨跌幅']].tail(15))
    
    # 应用策略
    result = is_selected(target_stock, combined_data)
    print(f"\n策略结果: {result}")
    
    # 添加调试信息来检查各个条件是否满足
    df = combined_data.copy()
    df['MA20_Volume'] = df['成交量'].rolling(window=20).mean()
    recent_days = df[df['代码'] == target_stock].tail(10)
    
    print(f"\n最近10天数据:")
    print(recent_days[['日期', '成交量', '涨跌幅', 'MA20_Volume']])
    
    # 检查高成交量条件
    high_volume_condition = (recent_days['成交量'] >= 4 * recent_days['MA20_Volume'])
    print(f"\n高成交量条件检查:")
    print(f"是否有交易日成交量达到20日均量的4倍以上: {high_volume_condition.any()}")
    if high_volume_condition.any():
        print("满足高成交量条件的日期:")
        print(recent_days[high_volume_condition][['日期', '成交量', 'MA20_Volume']])
    
    # 检查涨停次数条件
    limit_up_condition = recent_days['涨跌幅'] >= 9.9
    limit_up_count = limit_up_condition.sum()
    print(f"\n涨停次数条件检查:")
    print(f"最近10天内涨停次数: {limit_up_count}")
    if limit_up_count > 0:
        print("涨停日期:")
        print(recent_days[limit_up_condition][['日期', '涨跌幅']])
    
    # 检查连续缩量下跌条件
    recent_days_sorted = recent_days.sort_values('日期', ascending=True).reset_index(drop=True)
    volume_decrease = recent_days_sorted['成交量'].diff() < 0  # 成交量比前一天少
    price_decrease = recent_days_sorted['收盘'].diff() < 0      # 收盘价比前一天低
    shrink_decline = volume_decrease & price_decrease
    
    print(f"\n缩量下跌分析:")
    analysis_df = pd.DataFrame({
        '日期': recent_days_sorted['日期'],
        '成交量': recent_days_sorted['成交量'],
        '收盘价': recent_days_sorted['收盘'],
        '成交量减少': volume_decrease,
        '价格下跌': price_decrease,
        '缩量下跌': shrink_decline
    })
    print(analysis_df.to_string(index=False))
    
    # 计算连续缩量下跌天数
    consecutive_count = 0
    max_consecutive = 0
    
    for is_shrink_decline in shrink_decline:
        if is_shrink_decline:
            consecutive_count += 1
            max_consecutive = max(max_consecutive, consecutive_count)
        else:
            consecutive_count = 0
    
    print(f"\n连续缩量下跌天数: {max_consecutive}")
    
else:
    print(f"未找到股票 {target_stock} 的数据")