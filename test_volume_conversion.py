import pandas as pd
import os
from utils.data_loader import get_clean_snapshot_data

# 设置测试环境
os.environ['DEBUG'] = 'True'

# 强制刷新获取最新快照数据
print("=== 测试成交量单位转换 ===")
snapshot_df = get_clean_snapshot_data(force_refresh=True)

if snapshot_df is not None and '成交量' in snapshot_df.columns:
    # 显示样本数据
    print("\n=== 成交量样本数据 ===")
    sample = snapshot_df[['代码', '成交量']].head(5)
    print(sample.to_string(index=False))
    
    # 统计信息
    print("\n=== 成交量统计信息 ===")
    print(f"最小值: {snapshot_df['成交量'].min()}")
    print(f"最大值: {snapshot_df['成交量'].max()}")
    print(f"中位数: {snapshot_df['成交量'].median()}")
    print(f"单位: {'股' if snapshot_df['成交量'].median() > 1000 else '手'}")
else:
    print("无法获取快照数据或成交量列不存在")