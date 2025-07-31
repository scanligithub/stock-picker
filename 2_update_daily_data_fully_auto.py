# ------------------------------------------------------------------
# 2_update_daily_data_fully_auto.py (全自动智能更新终极版 - 带调试)
# 功能: 自动检测并补齐所有缺失的交易日数据，无需任何手动输入。
#       增加了可被外部程序解析的进度输出和用于单位调试的打印。
# ------------------------------------------------------------------
import tushare as ts
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import os
from tqdm import tqdm
import sys # 引入 sys 模块
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import signal
import threading
import argparse

# --- 配置 ---
MASTER_DATA_FILE = 'master_stock_data.feather'

# 全局变量用于处理中断信号
shutdown_event = threading.Event()

def signal_handler(signum, frame):
    """处理中断信号"""
    print("\n收到中断信号，正在优雅地关闭...", file=sys.stderr)
    shutdown_event.set()

def get_market_snapshot_data(stock_codes):
    """
    获取市场快照数据作为当天收盘数据
    """
    try:
        print("--- 正在获取市场快照数据... ---")
        # 获取A股市场实时数据
        snapshot_df = ak.stock_zh_a_spot_em()
        
        # 过滤出需要的股票
        filtered_data = []
        for code in stock_codes:
            # 生成带市场前缀的完整代码
            if code.startswith(('0', '3')):
                full_code = f"sz{code}"
            elif code.startswith(('6', '8', '9')):
                full_code = f"sh{code}"
            else:
                print(f"无法识别的股票代码格式: {code}，跳过该股票", file=sys.stderr)
                continue
            
            # 筛选对应股票的数据
            stock_data = snapshot_df[snapshot_df['代码'] == full_code]
            if not stock_data.empty:
                # 添加日期列
                stock_data = stock_data.copy()
                stock_data['trade_date'] = pd.to_datetime(datetime.now().date())
                stock_data['ts_code'] = full_code
                filtered_data.append(stock_data)
        
        if filtered_data:
            result_df = pd.concat(filtered_data, ignore_index=True)
            print(f"--- 成功获取 {len(filtered_data)} 只股票的市场快照数据 ---")
            return result_df
        else:
            print("--- 未能获取任何市场快照数据 ---")
            return None
    except Exception as e:
        print(f"!!! 获取市场快照数据失败: {e}")
        return None

def update_data_fully_auto(force_date=None):
    """
    全自动、智能化地检测并补齐所有缺失的交易日数据。
    """
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("--- 开始执行 update_data_fully_auto 函数 ---")
    
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

    # --- 2. 加载本地数据 ---
    try:
        master_df = pd.read_feather(MASTER_DATA_FILE)
        master_df['日期'] = pd.to_datetime(master_df['日期'])
        latest_local_date = master_df['日期'].max()
    except Exception as e:
        print(f"!!! 读取母版文件 '{MASTER_DATA_FILE}' 失败: {e}")
        return
    
    # 如果指定了强制更新日期
    if force_date:
        force_date_obj = pd.to_datetime(force_date)
        print(f"--- 强制更新日期: {force_date_obj.strftime('%Y-%m-%d')} ---")
        
        # 检查该日期数据是否已存在，如果存在则先删除
        if force_date_obj in master_df['日期'].values:
            print(f"--- 检测到 {force_date_obj.strftime('%Y-%m-%d')} 的数据已存在，正在删除旧数据... ---")
            master_df = master_df[master_df['日期'] != force_date_obj]
        
        # 设置待下载日期为强制更新日期
        dates_to_download = pd.Series([force_date_obj])
        print("--- 设置待下载日期为强制更新日期 ---")
        
        # 如果强制更新的日期是今天，则优先尝试获取市场快照数据
        today = pd.to_datetime(datetime.now().date())
        if force_date_obj.date() == today.date():
            print("--- 强制更新日期为今天，优先尝试获取市场快照数据... ---")
            # 从stock_pool.csv加载股票池
            try:
                stock_pool = pd.read_csv('stock_pool.csv')
                # 修正股票代码提取逻辑，确保正确处理原始数据格式
                stock_codes = stock_pool['ts_code'].str.extract(r'(\d{6})')[0].dropna().tolist()
                print(f"--- 从stock_pool.csv加载了 {len(stock_codes)} 只股票 --- ")
            except Exception as e:
                print(f"!!! 加载股票池失败，使用默认股票池: {e}", file=sys.stderr)
                stock_codes = ['000001', '600519']  # 平安银行和贵州茅台
            
            # 尝试获取市场快照数据
            snapshot_data = get_market_snapshot_data(stock_codes)
            if snapshot_data is not None:
                print("--- 市场快照数据获取成功，跳过历史数据下载 ---")
                # 直接保存数据并返回，不进入历史数据下载逻辑
                
                # --- 合并、格式化并保存 ---
                print("\n--- 数据补齐完成，正在合并到母版文件... ---", file=sys.stderr)
                new_data_df = snapshot_data
                
                rename_map = {
                    'ts_code': '代码', 'trade_date': '日期', 'open': '开盘', 'close': '收盘', 
                    'high': '最高', 'low': '最低', 'amount': '成交额'
                }
                new_data_df.rename(columns=rename_map, inplace=True)
                
                # 单位转换
                new_data_df['成交额'] *= 1000
                
                final_columns = ['代码', '日期', '开盘', '收盘', '最高', '最低', '成交额']
                new_data_df = new_data_df[final_columns]
                
                # 合并到主 DataFrame
                updated_df = pd.concat([master_df, new_data_df], ignore_index=True)
                
                # 再次清洗和排序
                updated_df['日期'] = pd.to_datetime(updated_df['日期'])
                updated_df.drop_duplicates(subset=['代码', '日期'], keep='last', inplace=True)
                updated_df.sort_values(by=['代码', '日期'], inplace=True)
                
                # 保存到文件
                updated_df.reset_index(drop=True).to_feather(MASTER_DATA_FILE)
                print(f"--- 母版文件更新成功！最新日期为: {updated_df['日期'].max().strftime('%Y-%m-%d')} ---", file=sys.stderr)
                # 不再直接返回，而是继续执行后续代码
                # return
            else:
                print("--- 市场快照数据获取失败，将继续使用历史数据下载逻辑 ---")
        # 如果强制更新的日期不是今天，仍然需要获取交易日历以检查日期是否有效
        else:
            # --- 3. 获取交易日历 --- 
            print("--- 正在使用Akshare获取交易日历以检测缺失日期...")
            try:
                trade_dates_df = ak.tool_trade_date_hist_sina()
                trade_dates_df['trade_date'] = pd.to_datetime(trade_dates_df['trade_date'])
                
                # 排除今天（当日数据可能尚未更新）
                today = pd.to_datetime(datetime.now().date())
                trade_dates_df = trade_dates_df[trade_dates_df['trade_date'] < today]
                print("--- 交易日历获取成功 ---")
                
                # 检查强制更新日期是否在交易日历中
                if force_date_obj not in trade_dates_df['trade_date'].values:
                    print(f"!!! 指定的强制更新日期 {force_date_obj.strftime('%Y-%m-%d')} 不在交易日历中，无法更新。")
                    return
            except Exception as e:
                print(f"!!! 获取交易日历失败: {e}")
                return
            
            # 保留原来的缺失日期检测逻辑，但将强制更新日期也加入待下载列表
            missing_dates = trade_dates_df[
                (trade_dates_df['trade_date'] > latest_local_date) & 
                (trade_dates_df['trade_date'] <= today)
            ]['trade_date']
            
            # 将强制更新日期添加到待下载列表中
            dates_to_download = pd.concat([dates_to_download, missing_dates], ignore_index=True).drop_duplicates()

        if dates_to_download.empty and not force_date:
            print(f"--- 数据已是最新，无需更新。最新日期: {latest_local_date.strftime('%Y-%m-%d')} ---")
            print("PROGRESS: 100", flush=True)
            return
        else:
            print("--- 有待下载的日期 ---")
        
        dates_to_download_str = [d.strftime('%Y%m%d') for d in dates_to_download]
        if not force_date:
            print(f"--- 检测到 {len(dates_to_download_str)} 个缺失的交易日，将使用Tushare进行补齐... ---")
            print(f"--- 缺失日期列表: {', '.join(dates_to_download_str)} ---")
        else:
            print(f"--- 准备更新日期: {', '.join(dates_to_download_str)} ---")
    
        new_data_list = []
        
        # --- 4. 遍历下载 ---
        print(f"--- 开始下载 {len(dates_to_download_str)} 个交易日数据... --- ")
        
        # 生产模式：使用动态检测的股票池和缺失日期
        # 从stock_pool.csv加载股票池
        try:
            stock_pool = pd.read_csv('stock_pool.csv')
            # 修正股票代码提取逻辑，确保正确处理原始数据格式
            stock_codes = stock_pool['ts_code'].str.extract(r'(\d{6})')[0].dropna().tolist()
            print(f"--- 从stock_pool.csv加载了 {len(stock_codes)} 只股票 --- ")
        except Exception as e:
            print(f"!!! 加载股票池失败，使用默认股票池: {e}", file=sys.stderr)
            stock_codes = ['000001', '600519']  # 平安银行和贵州茅台
        
        # 优先尝试获取当天的市场快照数据
        today = pd.to_datetime(datetime.now().date())
        if today in dates_to_download.values:
            print("--- 尝试获取当天市场快照数据... ---")
            snapshot_data = get_market_snapshot_data(stock_codes)
            if snapshot_data is not None:
                new_data_list.append(snapshot_data)
                # 从待下载日期列表中移除今天
                dates_to_download = dates_to_download[dates_to_download != today]
                dates_to_download_str = [d.strftime('%Y%m%d') for d in dates_to_download]
                print(f"--- 市场快照数据获取成功，剩余 {len(dates_to_download_str)} 个历史交易日需要下载 ---")
        
        # 如果还有其他日期需要下载，则使用Tushare历史数据下载逻辑
        if not dates_to_download.empty:
            dates_to_download_str = [d.strftime('%Y%m%d') for d in dates_to_download]
            # 使用Tushare遍历下载每一个缺失日期的数据
            for date_str in tqdm(dates_to_download_str, desc="Tushare补齐数据"):
                try:
                    # 检查是否需要关闭
                    if shutdown_event.is_set():
                        print("\n正在中断下载任务...", file=sys.stderr)
                        break
                        
                    # 使用我们验证过有权限的Tushare daily接口
                    daily_data = pro.daily(trade_date=date_str)
                    if not daily_data.empty:
                        new_data_list.append(daily_data)
                        print(f"--- 成功下载 {date_str} 的数据，共 {len(daily_data)} 条记录 ---")
                    else:
                        print(f"--- 警告: Tushare返回 {date_str} 的数据为空，可能是非交易日或数据尚未更新 ---")
                except Exception as e:
                    tqdm.write(f"下载 {date_str} 数据时失败: {e}，将跳过。")
                    continue
        
        if not new_data_list:
            print("!!! 未能下载任何缺失的数据。程序退出。")
            return
    
        # --- 5. 合并、格式化并保存 ---
        print("\n--- 数据补齐完成，正在合并到母版文件... ---", file=sys.stderr)
        new_data_df = pd.concat(new_data_list, ignore_index=True)
        
        # 统一列名和单位 (与脚本1保持一致)
        # 注意：这里根据数据来源不同，列名可能不同
        # 市场快照数据使用不同的列名
        if 'vol' in new_data_df.columns:
            # 来自Tushare的数据
            rename_map = {'ts_code': '代码', 'trade_date': '日期', 'open': '开盘', 'close': '收盘', 'high': '最高', 'low': '最低', 'vol': '成交量', 'amount': '成交额'}
            new_data_df.rename(columns=rename_map, inplace=True)
            new_data_df['成交量'] *= 100
        else:
            # 来自市场快照的数据
            rename_map = {'ts_code': '代码', 'trade_date': '日期', 'open': '开盘', 'close': '收盘', 'high': '最高', 'low': '最低', 'amount': '成交额'}
            new_data_df.rename(columns=rename_map, inplace=True)
        
        # 所有数据都需要成交额单位转换
        new_data_df['成交额'] *= 1000
        
        # 确定最终列
        if '成交量' in new_data_df.columns:
            final_columns = ['代码', '日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额']
        else:
            final_columns = ['代码', '日期', '开盘', '收盘', '最高', '最低', '成交额']
            
        new_data_df = new_data_df[final_columns]
    
        # 合并到主 DataFrame
        updated_df = pd.concat([master_df, new_data_df], ignore_index=True)
        
        # 再次清洗和排序
        updated_df['日期'] = pd.to_datetime(updated_df['日期'])
        updated_df.drop_duplicates(subset=['代码', '日期'], keep='last', inplace=True)
        updated_df.sort_values(by=['代码', '日期'], inplace=True)
        
        # 保存到文件
        updated_df.reset_index(drop=True).to_feather(MASTER_DATA_FILE)
        print(f"--- 母版文件更新成功！最新日期为: {updated_df['日期'].max().strftime('%Y-%m-%d')} ---", file=sys.stderr)
    else:
        # --- 3. 使用Akshare获取交易日历，识别缺失日期 ---
        print("--- 正在使用Akshare获取交易日历以检测缺失日期...")
        try:
            # 这个函数免费且稳定
            trade_dates_df = ak.tool_trade_date_hist_sina()
            trade_dates_df['trade_date'] = pd.to_datetime(trade_dates_df['trade_date'])
            
            # 排除今天（当日数据可能尚未更新）
            today = pd.to_datetime(datetime.now().date())
            trade_dates_df = trade_dates_df[trade_dates_df['trade_date'] < today]
            print("--- 交易日历获取成功 ---")
            
        except Exception as e:
            print(f"!!! 获取交易日历失败: {e}")
            return
        
        # 筛选缺失日期
        missing_dates = trade_dates_df[
            (trade_dates_df['trade_date'] > latest_local_date) & 
            (trade_dates_df['trade_date'] <= today)
        ]['trade_date']
        
        if missing_dates.empty:
            print(f"--- 数据已是最新，无需更新。最新日期: {latest_local_date.strftime('%Y-%m-%d')} ---")
            print("PROGRESS: 100", flush=True)
            return
        
        print(f"--- 检测到 {len(missing_dates)} 个缺失的交易日，将使用Tushare进行补齐... ---")
        missing_dates_str = [d.strftime('%Y%m%d') for d in missing_dates]
        print(f"--- 缺失日期列表: {', '.join(missing_dates_str)} ---")
        
        new_data_list = []
        
        # --- 4. 遍历下载 ---
        print(f"--- 开始下载 {len(missing_dates_str)} 个交易日数据... --- ")
        
        # 生产模式：使用动态检测的股票池和缺失日期
        # 从stock_pool.csv加载股票池
        try:
            stock_pool = pd.read_csv('stock_pool.csv')
            # 修正股票代码提取逻辑，确保正确处理原始数据格式
            stock_codes = stock_pool['ts_code'].str.extract(r'(\d{6})')[0].dropna().tolist()
            print(f"--- 从stock_pool.csv加载了 {len(stock_codes)} 只股票 --- ")
        except Exception as e:
            print(f"!!! 加载股票池失败，使用默认股票池: {e}", file=sys.stderr)
            stock_codes = ['000001', '600519']  # 平安银行和贵州茅台
        
        # 优先尝试获取当天的市场快照数据
        today = pd.to_datetime(datetime.now().date())
        if today in missing_dates.values:
            print("--- 尝试获取当天市场快照数据... ---")
            snapshot_data = get_market_snapshot_data(stock_codes)
            if snapshot_data is not None:
                new_data_list.append(snapshot_data)
                # 从待下载日期列表中移除今天
                missing_dates = missing_dates[missing_dates != today]
                missing_dates_str = [d.strftime('%Y%m%d') for d in missing_dates]
                print(f"--- 市场快照数据获取成功，剩余 {len(missing_dates_str)} 个历史交易日需要下载 ---")
        
        # 如果还有其他日期需要下载，则使用Tushare历史数据下载逻辑
        if not missing_dates.empty:
            missing_dates_str = [d.strftime('%Y%m%d') for d in missing_dates]
            # 使用Tushare遍历下载每一个缺失日期的数据
            for date_str in tqdm(missing_dates_str, desc="Tushare补齐数据"):
                try:
                    # 检查是否需要关闭
                    if shutdown_event.is_set():
                        print("\n正在中断下载任务...", file=sys.stderr)
                        break
                        
                    # 使用我们验证过有权限的Tushare daily接口
                    daily_data = pro.daily(trade_date=date_str)
                    if not daily_data.empty:
                        new_data_list.append(daily_data)
                except Exception as e:
                    tqdm.write(f"下载 {date_str} 数据时失败: {e}，将跳过。")
                    continue
        
        if not new_data_list:
            print("!!! 未能下载任何缺失的数据。程序退出。")
            return
    
        # --- 5. 合并、格式化并保存 ---
        print("\n--- 数据补齐完成，正在合并到母版文件... ---", file=sys.stderr)
        new_data_df = pd.concat(new_data_list, ignore_index=True)
        
        # 统一列名和单位 (与脚本1保持一致)
        # 注意：这里根据数据来源不同，列名可能不同
        # 市场快照数据使用不同的列名
        if 'vol' in new_data_df.columns:
            # 来自Tushare的数据
            rename_map = {'ts_code': '代码', 'trade_date': '日期', 'open': '开盘', 'close': '收盘', 'high': '最高', 'low': '最低', 'vol': '成交量', 'amount': '成交额'}
            new_data_df.rename(columns=rename_map, inplace=True)
            new_data_df['成交量'] *= 100
        else:
            # 来自市场快照的数据
            rename_map = {'ts_code': '代码', 'trade_date': '日期', 'open': '开盘', 'close': '收盘', 'high': '最高', 'low': '最低', 'amount': '成交额'}
            new_data_df.rename(columns=rename_map, inplace=True)
        
        # 所有数据都需要成交额单位转换
        new_data_df['成交额'] *= 1000
        
        # 确定最终列
        if '成交量' in new_data_df.columns:
            final_columns = ['代码', '日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额']
        else:
            final_columns = ['代码', '日期', '开盘', '收盘', '最高', '最低', '成交额']
            
        new_data_df = new_data_df[final_columns]
    
        # 合并到主 DataFrame
        updated_df = pd.concat([master_df, new_data_df], ignore_index=True)
        
        # 再次清洗和排序
        updated_df['日期'] = pd.to_datetime(updated_df['日期'])
        updated_df.drop_duplicates(subset=['代码', '日期'], keep='last', inplace=True)
        updated_df.sort_values(by=['代码', '日期'], inplace=True)
        
        # 保存到文件
        updated_df.reset_index(drop=True).to_feather(MASTER_DATA_FILE)
        print(f"--- 母版文件更新成功！最新日期为: {updated_df['日期'].max().strftime('%Y-%m-%d')} ---", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(description='全自动智能更新股票数据')
    parser.add_argument('--force-date', type=str, help='强制更新指定日期的数据 (格式: YYYY-MM-DD)')
    args = parser.parse_args()
    
    update_data_fully_auto(args.force_date)

if __name__ == "__main__":
    main()