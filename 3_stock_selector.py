# 3_stock_selector.py
import os
import pandas as pd
from tqdm import tqdm
import akshare as ak
from datetime import datetime, timedelta
import sys
import traceback
from utils.data_loader import load_clean_hist_data, get_clean_snapshot_data

# --- (假设策略和配置部分不变) ---
try:
    from strategies import STRATEGIES
    from config import SELECTED_STRATEGY
except ImportError:
    def mock_strategy(stock_code, df):
        if not df.empty and '涨跌幅' in df.columns:
            latest_change = df.iloc[-1]['涨跌幅']
            if pd.notna(latest_change) and latest_change > 2:
                return True, df.iloc[-1]['日期'], latest_change
        return False, None, None
    STRATEGIES = {"mock_strategy": mock_strategy}
    try:
        from config import SELECTED_STRATEGY
    except ImportError:
        SELECTED_STRATEGY = "mock_strategy"
        print("警告: 未找到 'strategies.py' 或 'config.py'，使用内置的模拟策略。", file=sys.stderr)


def main():
    """主程序入口"""
    # 重定向stderr到stdout，确保GUI能捕获所有输出
    sys.stderr = sys.stdout
    print("--- 正在从本地加载历史数据... ---", file=sys.stderr)
    hist_data_full = load_clean_hist_data()
    if hist_data_full is None:
        print("!!! 加载历史数据失败", file=sys.stderr)
        return

    print(f"--- 数据加载成功！共 {len(hist_data_full['代码'].unique())} 只股票的历史数据。", file=sys.stderr)

    print("--- 正在获取今日行情快照... ---", file=sys.stderr)
    snapshot_df = get_clean_snapshot_data()
    if snapshot_df is None:
        print("!!! 获取快照失败，退出", file=sys.stderr)
        return

    print("\n[OK] 开始统一字段定义...", file=sys.stderr)
    aligned_hist, aligned_snapshot = align_fields(hist_data_full, snapshot_df)
    print("[OK] 字段已统一，开始执行选股策略", file=sys.stderr)

    selected_stocks = []
    now = datetime.now()
    is_market_closed = now.hour >= 15
    today = now.date()
    # For the MA strategy, we need at least 60 days of historical data
    # For other strategies, we can use a weekly range
    if SELECTED_STRATEGY == "ma_condition_strategy":
        start_date = datetime.combine(today - timedelta(days=60), datetime.min.time())
    elif SELECTED_STRATEGY == "week_ma_arrangement":
        # 对于周K线多头排列策略，需要至少300天的数据来计算周线MA30
        start_date = datetime.combine(today - timedelta(days=300), datetime.min.time())
    else:
        monday_of_week = today - timedelta(days=today.weekday())
        start_date = datetime.combine(monday_of_week, datetime.min.time())

    print(f"\n--- 当前分析周期为: {start_date.strftime('%Y-%m-%d')} 至 {today.strftime('%Y-%m-%d')} ---\n", file=sys.stderr)

    try:
        stock_pool = pd.read_csv('stock_pool.csv')
        code_name_map = {str(row['ts_code']).upper(): row['name'] for _, row in stock_pool.iterrows()}
    except FileNotFoundError:
        print("!!! 警告: 'stock_pool.csv' 未找到，股票名称可能无法显示。", file=sys.stderr)
        code_name_map = {}

    strategy_func = STRATEGIES.get(SELECTED_STRATEGY)
    if not strategy_func:
        print(f"!!! 错误: 在 STRATEGIES 中未找到名为 '{SELECTED_STRATEGY}' 的策略。", file=sys.stderr)
        return

    grouped = aligned_hist.groupby('代码')
    total_stocks = len(grouped)
    
    # ===== 关键修正：配置 tqdm 在非终端环境下安全运行 =====
    progress_bar = tqdm(
        grouped, 
        total=total_stocks, 
        desc="[Analyzing Stocks]", 
        file=sys.stderr,
        mininterval=1.0,  # 每秒最多更新一次终端进度条
        disable=not sys.stderr.isatty() # 如果 stderr 不是一个终端，则完全禁用 tqdm 的视觉输出
    )

    UPDATE_INTERVAL = 50 

    for i, (stock_code, hist_data) in enumerate(progress_bar):
        try:
            # For the MA strategy, we need to ensure we have at least 60 data points
            if SELECTED_STRATEGY == "ma_condition_strategy":
                # Sort by date and take the last 60 data points
                this_week_hist = hist_data.sort_values('日期').tail(60).copy()
            # For the high volume strategy, we need at least 30 data points to calculate 20-day MA
            elif SELECTED_STRATEGY == "high_volume_strategy":
                # Sort by date and take the last 30 data points
                this_week_hist = hist_data.sort_values('日期').tail(30).copy()
            # For the week MA arrangement strategy, we need at least 300 data points
            elif SELECTED_STRATEGY == "week_ma_arrangement":
                # Sort by date and take the last 300 data points
                this_week_hist = hist_data.sort_values('日期').tail(300).copy()
            else:
                this_week_hist = hist_data[(hist_data['日期'] >= start_date) & (hist_data['日期'] < datetime.combine(today, datetime.min.time()))].copy()
            
            today_snapshot = snapshot_df[snapshot_df['代码'] == str(stock_code).upper()]
            if today_snapshot.empty: 
                continue
            today_snapshot = today_snapshot.assign(日期=pd.to_datetime(today))
            # 确保合并时包含成交量数据
            hist_columns = ['日期', '涨跌幅', '收盘']
            if '成交量' in this_week_hist.columns and '成交量' in today_snapshot.columns:
                hist_columns.append('成交量')
            this_week_combined = pd.concat([this_week_hist[hist_columns], today_snapshot[hist_columns]], ignore_index=True)
            # 仅对历史数据去重，保留今日快照数据
            # 修改去重逻辑：优先保留有成交量数据的记录
            if '成交量' in this_week_combined.columns:
                # 创建一个标记列，标识哪些行有成交量数据
                this_week_combined['has_volume'] = this_week_combined['成交量'].notna()
                # 按日期分组，优先保留有成交量的记录
                this_week_combined = this_week_combined.sort_values(['日期', 'has_volume'], ascending=[True, False]).drop_duplicates(subset=['日期'], keep='first')
                # 删除辅助列
                this_week_combined.drop(columns=['has_volume'], inplace=True)
            else:
                this_week_combined.drop_duplicates(subset=['日期'], keep='last', inplace=True)
            this_week_combined.sort_values(by='日期', inplace=True)
            latest_close = today_snapshot.iloc[-1].get('收盘')
            if pd.isna(latest_close): continue
            result = strategy_func(stock_code, this_week_combined)
            # 处理策略返回的不同格式
            if isinstance(result, tuple):
                # 确保至少有3个元素
                if len(result) >= 3:
                    # 新格式 (is_selected_flag, today_volume, yesterday_volume, ...)
                    is_selected_flag = result[0]
                    today_volume = result[1] if len(result) > 1 else None
                    yesterday_volume = result[2] if len(result) > 2 else None
                    # 如果有额外的参数，假设第4个是trigger_date，第5个是change_percent
                    trigger_date = result[3] if len(result) > 3 else None
                    change_percent = result[4] if len(result) > 4 else None
                else:
                    is_selected_flag = result[0] if result else False
                    today_volume = yesterday_volume = trigger_date = change_percent = None
            elif isinstance(result, bool):
                is_selected_flag, trigger_date, change_percent = result, None, None
                today_volume = yesterday_volume = None
            else:
                is_selected_flag = False
                today_volume = yesterday_volume = trigger_date = change_percent = None
            
            if is_selected_flag:
                result_dict = {
                    'ts_code': stock_code, 
                    '名称': code_name_map.get(stock_code, ''), 
                    '最后触发日期': trigger_date.strftime('%Y-%m-%d') if isinstance(trigger_date, (datetime, pd.Timestamp)) else today.strftime('%Y-%m-%d'), 
                    '当前股价': round(latest_close, 2), 
                    '涨跌幅%': round(change_percent, 2) if pd.notna(change_percent) else None,
                    '当天成交量': int(today_volume) if today_volume is not None else None,
                    '上一交易日成交量': int(yesterday_volume) if yesterday_volume is not None else None
                }
                selected_stocks.append(result_dict)
        except Exception as e:
            progress_bar.write(f"Error processing {stock_code}: {e}")
            traceback.print_exc(file=sys.stderr)
            continue
        finally:
            if (i + 1) % UPDATE_INTERVAL == 0 or (i + 1) == total_stocks:
                progress_percentage = int(((i + 1) / total_stocks) * 100)
                print(f"PROGRESS: {progress_percentage}", flush=True)

    print(f"\n\n==============================================", file=sys.stderr)
    print(f"         {'Post-market' if is_market_closed else 'Intraday'} Selection Results         ", file=sys.stderr)
    print("==============================================", file=sys.stderr)
    if not selected_stocks:
        print(f"No stocks found matching the criteria.", file=sys.stderr)
    else:
        result_df = pd.DataFrame(selected_stocks)
        result_df.sort_values(by='ts_code', inplace=True)
        # 使用绝对路径保存选股结果
        project_root = os.path.dirname(os.path.abspath(__file__))
        output_filename = os.path.join(project_root, 'selected_stocks.csv')
        try:
            print(f"[DEBUG] 尝试保存选股结果到: {output_filename}", file=sys.stderr)
            print(f"[DEBUG] 结果数据量: {len(result_df)} 行", file=sys.stderr)
            result_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # 验证文件是否创建成功
            if os.path.exists(output_filename):
                print(f"[SUCCESS] 选股结果已保存: {output_filename} (大小: {os.path.getsize(output_filename)} bytes)", file=sys.stderr)
            else:
                print(f"[ERROR] 保存失败! 文件未创建", file=sys.stderr)
        except Exception as e:
            print(f"\nFailed to save results file: {e}", file=sys.stderr)

        # 保存快照数据
        # 保存为Feather格式到项目根目录
        snapshot_cache_path = os.path.join(project_root, 'snapshot_cache.feather')
        try:
            snapshot_df.to_feather(snapshot_cache_path)
            if os.path.exists(snapshot_cache_path):
                print(f"[SUCCESS] 快照数据已缓存: {snapshot_cache_path}", file=sys.stderr)
            else:
                print(f"[ERROR] 快照缓存保存失败! 文件未创建", file=sys.stderr)
            # 同时保存为CSV格式
            snapshot_csv_path = os.path.join(project_root, 'snapshot_cache.csv')
            try:
                snapshot_df.to_csv(snapshot_csv_path, index=False, encoding='utf-8-sig')
                if os.path.exists(snapshot_csv_path):
                    print(f"[SUCCESS] 快照CSV已保存: {snapshot_csv_path}", file=sys.stderr)
                else:
                    print(f"[ERROR] 快照CSV保存失败! 文件未创建", file=sys.stderr)
            except Exception as e:
                print(f"保存快照CSV失败: {e}", file=sys.stderr)
        except Exception as e:
            print(f"保存快照缓存失败: {e}", file=sys.stderr)

        print(result_df.to_string(index=False), file=sys.stderr)
        print(f"\nTask complete. Found {len(result_df)} matching stocks.", file=sys.stderr)
    print("==============================================", file=sys.stderr)
    
def align_fields(hist_df, snapshot_df):
    all_columns = set(hist_df.columns).union(set(snapshot_df.columns))
    aligned_hist, aligned_snapshot = hist_df.copy(), snapshot_df.copy()
    for col in all_columns:
        if col not in aligned_hist.columns: aligned_hist[col] = None
        if col not in aligned_snapshot.columns: aligned_snapshot[col] = None
    aligned_hist = aligned_hist[sorted(list(all_columns))]
    aligned_snapshot = aligned_snapshot[sorted(list(all_columns))]
    return aligned_hist, aligned_snapshot

if __name__ == "__main__":
    main()