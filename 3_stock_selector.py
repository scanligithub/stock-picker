# 3_stock_selector.py

import os
import pandas as pd
from tqdm import tqdm
import akshare as ak
from datetime import datetime, timedelta

from strategies import STRATEGIES
from config import SELECTED_STRATEGY


def main():
    """主程序入口"""
    print("--- 正在从本地加载历史数据... ---")
    hist_data_full = load_hist_data()
    if hist_data_full is None:
        print("!!! 加载历史数据失败")
        return

    print(f"--- 数据加载成功！共 {len(hist_data_full['代码'].unique())} 只股票的历史数据。")

    print("--- 正在获取今日行情快照... ---")
    snapshot_df = get_snapshot_data()
    if snapshot_df is None:
        print("!!! 获取快照失败，退出")
        return

    print("\n📊 字段对比：")
    print(f"{'字段':<8} {'历史数据':<6} {'快照数据':<6}")
    print("-" * 30)

    all_columns = set(hist_data_full.columns).union(set(snapshot_df.columns))
    for col in sorted(all_columns):
        has_hist = "✅" if col in hist_data_full.columns else "❌"
        has_snap = "✅" if col in snapshot_df.columns else "❌"
        print(f"{col:<10} {has_hist:<6} {has_snap:<6}")

    print("\n✅ 开始统一字段定义...")
    aligned_hist, aligned_snapshot = align_fields(hist_data_full, snapshot_df)
    print("✅ 字段已统一，开始执行选股策略")

    selected_stocks = []

    # 获取当前时间并判断是否已收盘
    now = datetime.now()
    is_market_closed = now.hour >= 15 and now.minute >= 5
    today = now.date()
    monday_of_week = today - timedelta(days=today.weekday())  # 周一
    start_date = datetime.combine(monday_of_week, datetime.min.time())

    print(f"\n--- 当前分析周期为: {start_date.strftime('%Y-%m-%d')} 至 {today.strftime('%Y-%m-%d')} ---\n")

    stock_pool = pd.read_csv('stock_pool.csv')
    code_name_map = {str(row['ts_code']).upper(): row['name'] for _, row in stock_pool.iterrows()}

    strategy_func = STRATEGIES[SELECTED_STRATEGY]

    grouped = aligned_hist.groupby('代码')

    for stock_code, hist_data in tqdm(grouped, total=len(grouped), desc="📈 分析股票"):
        try:
            # 筛选过去 N 天的数据
            this_week_hist = hist_data[
                (hist_data['日期'] >= start_date) &
                (hist_data['日期'] < datetime.combine(today, datetime.min.time()))
            ].copy()

            # 获取今天的快照数据
            today_snapshot = aligned_snapshot[aligned_snapshot['代码'] == str(stock_code).upper()]

            if today_snapshot.empty:
                continue

            # 添加今天的日期字段
            today_snapshot = today_snapshot.assign(日期=pd.to_datetime(today))

            # 合并历史 + 快照数据
            this_week_combined = pd.concat([
                this_week_hist[['日期', '涨跌幅']],
                today_snapshot[['日期', '涨跌幅']]
            ], ignore_index=True).drop_duplicates(subset=['日期'])

            this_week_combined.sort_values(by='日期', inplace=True)

            # 获取最新收盘价
            latest_close = None
            if not today_snapshot.empty:
                latest_close = today_snapshot.iloc[-1].get('收盘', None)
            elif not this_week_hist.empty:
                latest_close = this_week_hist.iloc[-1].get('收盘', None)

            if latest_close is None:
                continue

            # 执行选股策略（返回值：is_selected_flag, trigger_date, change_percent）
            result = strategy_func(stock_code, this_week_combined)

            # 解析返回值（兼容旧版 bool 返回格式）
            if isinstance(result, tuple) and len(result) == 3:
                is_selected_flag, trigger_date, change_percent = result
            elif isinstance(result, bool):
                is_selected_flag = result
                trigger_date = None
                change_percent = None
            else:
                is_selected_flag = False
                trigger_date = None
                change_percent = None

            # 如果满足条件，记录结果
            if is_selected_flag:
                result_dict = {
                    '代码': stock_code,
                    '名称': code_name_map.get(stock_code, ''),
                    '最后触发日期': trigger_date.strftime('%Y-%m-%d') if trigger_date else '',
                    '当前股价': round(latest_close, 2),
                    '涨跌幅%': round(change_percent, 2) if change_percent is not None else None
                }
                selected_stocks.append(result_dict)

        except Exception as e:
            print(f"🚨 处理 {stock_code} 出错：{e}")
            import traceback
            traceback.print_exc()
            continue

    # --- 输出最终结果 ---
    print(f"\n\n==============================================")
    print(f"         {'盘后' if is_market_closed else '盘中'} 选股结果         ")
    print("==============================================")

    if not selected_stocks:
        print(f"未找到任何符合条件的股票。")
    else:
        result_df = pd.DataFrame(selected_stocks)
        result_df.sort_values(by='代码', inplace=True)
        print(result_df.to_string(index=False))
        print(f"\n任务完成，共找出 {len(result_df)} 只符合条件的股票。")

    print("==============================================")


def load_hist_data():
    """加载历史数据"""
    MASTER_DATA_FILE = 'master_stock_data.feather'
    if not os.path.exists(MASTER_DATA_FILE):
        print(f"!!! 错误: 找不到母版数据文件 '{MASTER_DATA_FILE}'")
        return None

    df = pd.read_feather(MASTER_DATA_FILE)
    df['日期'] = pd.to_datetime(df['日期'])
    df['代码'] = df['代码'].astype(str).str.upper()
    return df


def get_snapshot_data():
    """获取今日行情快照，并补全市场标识符"""
    try:
        df = ak.stock_zh_a_spot_em()

        if df.empty:
            print("⚠️ 快照行情为空")
            return None

        # 自动识别标准字段名
        column_mapping = {
            '代码': ['代码', 'symbol'],
            '名称': ['名称', 'name'],
            '开盘': ['开盘', 'open'],
            '收盘': ['最新价', 'price', 'close'],
            '最高': ['最高', 'high'],
            '最低': ['最低', 'low'],
            '成交量': ['成交量', 'volume'],
            '成交额': ['成交额', 'amount'],
            '涨跌幅': ['涨跌幅', 'changepercent']
        }

        actual_columns = {}
        for target, candidates in column_mapping.items():
            for col in candidates:
                if col in df.columns:
                    actual_columns[col] = target
                    break

        if len(actual_columns) < 3:
            print("⚠️ 获取到的快照数据字段不足")
            return None

        df = df[list(actual_columns.keys())].copy()
        df.rename(columns=actual_columns, inplace=True)

        # 添加“日期”字段为今天
        today = datetime.now().date()
        df['日期'] = pd.to_datetime(today)

        # 补全 .SH/.SZ 后缀
        def add_market_suffix(code):
            code = str(code).upper()
            if code.startswith("6") or code.startswith("9"):
                return f"{code}.SH"
            elif code.startswith(("0", "3")):
                return f"{code}.SZ"
            else:
                return code

        df['代码'] = df['代码'].apply(add_market_suffix)

        # 保存缓存
        df.to_feather('snapshot_data.feather')
        print("💾 快照已缓存到 snapshot_data.feather")

        return df

    except Exception as e:
        print(f"❌ 获取快照失败: {e}")
        return None


def align_fields(hist_df, snapshot_df):
    """
    统一字段定义，确保两份数据字段一致
    """
    all_columns = set(hist_df.columns).union(set(snapshot_df.columns))

    aligned_hist = hist_df.copy()
    aligned_snapshot = snapshot_df.copy()

    # 补全缺失字段
    for col in all_columns:
        if col not in aligned_hist.columns:
            aligned_hist[col] = None
        if col not in aligned_snapshot.columns:
            aligned_snapshot[col] = None

    # 重新排序字段
    aligned_hist = aligned_hist[sorted(all_columns)]
    aligned_snapshot = aligned_snapshot[sorted(all_columns)]

    return aligned_hist, aligned_snapshot


if __name__ == "__main__":
    main()