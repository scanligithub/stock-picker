# ------------------------------------------------------------------
# 2_update_daily_data_fully_auto.py (全自动智能更新终极版)
# 功能: 自动检测并补齐所有缺失的交易日数据，无需任何手动输入。
# ------------------------------------------------------------------
import tushare as ts
import akshare as ak
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import os

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
    
    # --- 1. 初始化Tushare (因为我们需要用它来下载数据) ---
    token = os.getenv('TUSHARE_TOKEN')
    if not token:
        print("!!! 未在环境变量中找到Tushare Token，无法更新。")
        return
    ts.set_token(token)
    pro = ts.pro_api()
    print("--- Tushare接口初始化成功 ---")

    # --- 2. 加载本地数据，找到本地的最新日期 ---
    master_df = pd.read_feather(MASTER_DATA_FILE)
    master_df['日期'] = pd.to_datetime(master_df['日期'])
    latest_local_date = master_df['日期'].max()
    
    # --- 3. 使用Akshare获取交易日历，识别缺失日期 ---
    print("--- 正在使用Akshare获取交易日历以检测缺失日期...")
    try:
        # 这个函数免费且稳定
        trade_dates_df = ak.tool_trade_date_hist_sina()
        trade_dates_df['trade_date'] = pd.to_datetime(trade_dates_df['trade_date'])
    except Exception as e:
        print(f"!!! 获取交易日历失败: {e}"); return

    today = pd.to_datetime(datetime.now().date())
    dates_to_download = trade_dates_df[
        (trade_dates_df['trade_date'] > latest_local_date) & 
        (trade_dates_df['trade_date'] <= today)
    ]['trade_date']

    if dates_to_download.empty:
        print(f"--- 数据已是最新，无需更新。最新日期: {latest_local_date.strftime('%Y-%m-%d')} ---")
        return
    
    dates_to_download_str = [d.strftime('%Y%m%d') for d in dates_to_download]
    print(f"--- 检测到 {len(dates_to_download_str)} 个缺失的交易日，将使用Tushare进行补齐... ---")
    print(f"--- 缺失日期列表: {', '.join(dates_to_download_str)} ---")

    new_data_list = []
    # --- 4. 使用Tushare遍历下载每一个缺失日期的数据 ---
    for date_str in tqdm(dates_to_download_str, desc="Tushare补齐数据"):
        try:
            # 使用我们验证过有权限的Tushare daily接口
            daily_data = pro.daily(trade_date=date_str)
            if not daily_data.empty:
                new_data_list.append(daily_data)
        except Exception as e:
            tqdm.write(f"下载 {date_str} 数据时失败: {e}，将跳过。")
            continue
    
    if not new_data_list:
        print("!!! 未能下载任何缺失的数据。程序退出。"); return

    # --- 5. 合并并保存 ---
    print("\n--- 数据补齐完成，正在合并到母版文件... ---")
    new_data_df = pd.concat(new_data_list, ignore_index=True)
    
    # 统一列名和单位 (与脚本1保持一致)
    rename_map = {'ts_code': '代码', 'trade_date': '日期', 'open': '开盘', 'close': '收盘', 'high': '最高', 'low': '最低', 'vol': '成交量', 'amount': '成交额', 'pct_chg': '涨跌幅'}
    new_data_df.rename(columns=rename_map, inplace=True)
    new_data_df['成交量'] *= 100
    new_data_df['成交额'] *= 1000
    final_columns = ['代码', '日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '涨跌幅']
    new_data_df = new_data_df[final_columns]
    
    updated_df = pd.concat([master_df, new_data_df], ignore_index=True)
    
    updated_df['日期'] = pd.to_datetime(updated_df['日期'])
    updated_df.drop_duplicates(subset=['代码', '日期'], keep='last', inplace=True)
    updated_df.sort_values(by=['代码', '日期'], inplace=True)
    
    updated_df.reset_index(drop=True).to_feather(MASTER_DATA_FILE)
    print(f"--- 母版文件更新成功！最新日期为: {updated_df['日期'].max().strftime('%Y-%m-%d')} ---")

if __name__ == "__main__":
    update_data_fully_auto()