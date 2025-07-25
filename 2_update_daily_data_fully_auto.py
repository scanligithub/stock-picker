# ------------------------------------------------------------------
# 2_update_daily_data_fully_auto.py (全自动智能更新终极版 - 带调试)
# 功能: 自动检测并补齐所有缺失的交易日数据，无需任何手动输入。
#       增加了可被外部程序解析的进度输出和用于单位调试的打印。
# ------------------------------------------------------------------
import akshare as ak
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import os
import sys # 引入 sys 模块

# --- 配置 ---
MASTER_DATA_FILE = 'master_stock_data.feather'

def update_data_fully_auto():
    """
    全自动、智能化地检测并补齐所有缺失的交易日数据。
    """
    if not os.path.exists(MASTER_DATA_FILE):
        print(f"!!! 错误: 找不到母版文件'{MASTER_DATA_FILE}'。")
        print("!!! 请先运行脚本1进行初始化。")
        return

    print("--- 任务2 (全自动版): 开始智能增量更新... ---")
    

    # --- 2. 加载本地数据 ---
    try:
        master_df = pd.read_feather(MASTER_DATA_FILE)
        master_df['日期'] = pd.to_datetime(master_df['日期'])
        latest_local_date = master_df['日期'].max()
    except Exception as e:
        print(f"!!! 读取母版文件 '{MASTER_DATA_FILE}' 失败: {e}")
        return
    
    # --- 3. 获取交易日历 ---
    print("--- 正在使用Akshare获取交易日历以检测缺失日期...")
    try:
        trade_dates_df = ak.tool_trade_date_hist_sina()
        trade_dates_df['trade_date'] = pd.to_datetime(trade_dates_df['trade_date'])
    except Exception as e:
        print(f"!!! 获取交易日历失败: {e}")
        return

    today = pd.to_datetime(datetime.now().date())
    dates_to_download = trade_dates_df[
        (trade_dates_df['trade_date'] > latest_local_date) & 
        (trade_dates_df['trade_date'] <= today)
    ]['trade_date']

    if dates_to_download.empty:
        print(f"--- 数据已是最新，无需更新。最新日期: {latest_local_date.strftime('%Y-%m-%d')} ---")
        print("PROGRESS: 100", flush=True)
        return
    
    dates_to_download_str = [d.strftime('%Y%m%d') for d in dates_to_download]
    print(f"--- 检测到 {len(dates_to_download_str)} 个缺失的交易日，将进行补齐... ---")
    print(f"--- 缺失日期列表: {', '.join(dates_to_download_str)} ---")

    new_data_list = []
    
    # --- 4. 遍历下载 ---
    print("--- 已移除Tushare数据下载逻辑，请使用Akshare实现数据获取 ---")
    # 清空待下载列表以跳过数据获取环节
    dates_to_download_str = []
    
    if not new_data_list:
        print("!!! 未能下载任何缺失的数据。程序退出。")
        return

    # --- 5. 合并、格式化并保存 ---
    print("\n--- 数据补齐完成，正在合并到母版文件... ---", file=sys.stderr)
    new_data_df = pd.concat(new_data_list, ignore_index=True)
    
    rename_map = {
        'ts_code': '代码', 'trade_date': '日期', 'open': '开盘', 'close': '收盘', 
        'high': '最高', 'low': '最低', 'vol': '成交量', 'amount': '成交额', 'pct_chg': '涨跌幅'
    }
    new_data_df.rename(columns=rename_map, inplace=True)
    
    # 单位转换
    new_data_df['成交额'] *= 1000
    new_data_df['成交量'] *= 100  # 将手转换为股
    
    final_columns = ['代码', '日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '涨跌幅']
    new_data_df = new_data_df[final_columns]
    
    # ===== 调试打印 3: 打印新下载数据的成交量 (以000001.SZ为例) =====
    if not new_data_df.empty:
        print("\n--- [DEBUG] 新下载的历史数据 (new_data_df) 示例 (000001.SZ): ---")
        debug_stock_df = new_data_df[new_data_df['代码'] == '000001.SZ']
        if not debug_stock_df.empty:
            print(debug_stock_df[['日期', '收盘', '成交量']].tail(1))
        else:
            print("新下载数据中没有找到 000001.SZ")
    # =================================================================

    # 合并到主 DataFrame
    updated_df = pd.concat([master_df, new_data_df], ignore_index=True)
    
    # 再次清洗和排序
    updated_df['日期'] = pd.to_datetime(updated_df['日期'])
    updated_df.drop_duplicates(subset=['代码', '日期'], keep='last', inplace=True)
    updated_df.sort_values(by=['代码', '日期'], inplace=True)
    
    # ===== 调试打印 4: 打印最终母版文件中特定股票最后两天的数据 =====
    if len(updated_df) > 2:
        print("\n--- [DEBUG] 更新后母版文件 (updated_df) 中 000001.SZ 最后两行: ---")
        final_debug_df = updated_df[updated_df['代码'] == '000001.SZ']
        if len(final_debug_df) >= 2:
            print(final_debug_df[['日期', '收盘', '成交量']].tail(2))
        else:
            print("母版文件中 000001.SZ 数据不足两行")
    # ==================================================================

    # 保存到文件
    updated_df.reset_index(drop=True).to_feather(MASTER_DATA_FILE)
    print(f"--- 母版文件更新成功！最新日期为: {updated_df['日期'].max().strftime('%Y-%m-%d')} ---", file=sys.stderr)

if __name__ == "__main__":
    update_data_fully_auto()