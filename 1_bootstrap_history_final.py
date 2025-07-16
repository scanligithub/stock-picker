# ------------------------------------------------------------------
# strategy_n_limit_up_fixed_final.py
# 功能: 加载历史数据 + 获取快照 + 对齐字段 + 补全市场标识符 + 执行选股策略
# 特点: 完美兼容 feather 历史数据与 akshare 快照行情
# ------------------------------------------------------------------

import os
import pandas as pd
from tqdm import tqdm
import akshare as ak
from datetime import datetime, timedelta

# --- 配置 ---
MASTER_DATA_FILE = 'master_stock_data.feather'
STOCK_POOL_FILE = 'stock_pool.csv'
N_CONSECUTIVE_DAYS = 4  # 默认找4连板
DEBUG_STOCK_CODE = '000514.SZ'  # 设置你要调试的目标股票代码

def load_hist_data():
    """加载历史数据"""
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

        # 确保股票代码是字符串并补全 .SZ/.SH 后缀
        def add_market_suffix(code):
            code = str(code).upper()
            if code.startswith("6") or code.startswith("9"):
                return f"{code}.SH"
            elif code.startswith(("0", "3")):
                return f"{code}.SZ"
            else:
                return code

        df['代码'] = df['代码'].apply(add_market_suffix)

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
    start_of_week = today - timedelta(days=now.weekday())
    start_of_week = datetime.combine(start_of_week, datetime.min.time())

    print(f"\n--- 当前分析周期为: {start_of_week.strftime('%Y-%m-%d')} 至 {today.strftime('%Y-%m-%d')} ---\n")

    stock_pool = pd.read_csv(STOCK_POOL_FILE)
    code_name_map = {str(row['ts_code']).upper(): row['name'] for _, row in stock_pool.iterrows()}

    grouped = aligned_hist.groupby('代码')

    for stock_code, hist_data in tqdm(grouped, total=len(grouped), desc=f"{N_CONSECUTIVE_DAYS}连板策略计算中"):
        try:
            # 筛选出本周的数据（从周一到昨天）
            this_week_hist = hist_data[
                (hist_data['日期'] >= start_of_week) &
                (hist_data['日期'] < datetime.combine(today, datetime.min.time()))
            ].copy()

            if stock_code == DEBUG_STOCK_CODE:
                print(f"\n🔍 开始处理目标股票：{stock_code}")
                print(f"🔢 调试股票代码类型: {type(stock_code)}")
                print(f"✅ {stock_code} 历史数据长度: {len(this_week_hist)}")
                print(this_week_hist[['日期', '涨跌幅']])

            # 获取今天的快照数据
            today_snapshot = aligned_snapshot[aligned_snapshot['代码'] == str(stock_code).upper()]

            if stock_code == DEBUG_STOCK_CODE:
                print(f"🔢 快照数据中的示例代码: {aligned_snapshot['代码'].iloc[0]}")
                print(f"🔢 是否包含目标股票？{'是' if not today_snapshot.empty else '否'}")

            if today_snapshot.empty:
                if stock_code == DEBUG_STOCK_CODE:
                    print(f"⚠️ {stock_code} 今日无快照数据")
                continue

            # 添加今天的日期字段
            today_snapshot = today_snapshot.assign(日期=pd.to_datetime(today))

            if stock_code == DEBUG_STOCK_CODE:
                print(f"✅ {stock_code} 快照数据：")
                print(today_snapshot[['日期', '涨跌幅']])

            # 如果是盘中运行，则不使用今天的数据参与判断
            if not is_market_closed:
                this_week_combined = this_week_hist.copy()
            else:
                this_week_combined = pd.concat([
                    this_week_hist[['日期', '涨跌幅']],
                    today_snapshot[['日期', '涨跌幅']]
                ], ignore_index=True).drop_duplicates(subset=['日期'])

            this_week_combined.sort_values(by='日期', inplace=True)

            if stock_code == DEBUG_STOCK_CODE:
                print(f"📊 {stock_code} 合并后的数据：")
                print(this_week_combined[['日期', '涨跌幅']])

            # 检查是否构成N连板
            if len(this_week_combined) < N_CONSECUTIVE_DAYS:
                if stock_code == DEBUG_STOCK_CODE:
                    print(f"❌ {stock_code} 数据不足 {N_CONSECUTIVE_DAYS} 天，跳过。")
                continue

            check_data = this_week_combined.tail(N_CONSECUTIVE_DAYS).copy()

            def is_limit_up(code, pct_change):
                if pct_change is None or pd.isna(pct_change):
                    return False
                if str(code).startswith(('30', '68')): return pct_change >= 19.8
                else: return pct_change >= 9.9

            check_data['is_zt'] = check_data.apply(
                lambda row: is_limit_up(stock_code, row['涨跌幅']),
                axis=1
            )

            if stock_code == DEBUG_STOCK_CODE:
                print(f"🎯 {stock_code} 是否涨停判断结果：")
                print(check_data[['日期', '涨跌幅', 'is_zt']])

            if check_data['is_zt'].all():
                result = {
                    '代码': stock_code,
                    '名称': code_name_map.get(stock_code, ''),
                    '连板结束日期': check_data['日期'].iloc[-1].strftime('%Y-%m-%d'),
                    f'连续{N_CONSECUTIVE_DAYS}日涨幅%': check_data['涨跌幅'].to_list()
                }
                selected_stocks.append(result)

        except Exception as e:
            if stock_code == DEBUG_STOCK_CODE:
                print(f"🚨 处理 {stock_code} 出错：{e}")
            continue

    # --- 输出最终结果 ---
    print(f"\n\n==============================================")
    print(f"         {'盘后' if is_market_closed else '盘中'} {N_CONSECUTIVE_DAYS} 连板股票列表         ")
    print("==============================================")

    if not selected_stocks:
        print(f"未找到任何 {N_CONSECUTIVE_DAYS} 连板的股票。")
    else:
        result_df = pd.DataFrame(selected_stocks)
        result_df.sort_values(by='代码', inplace=True)
        print(result_df.to_string(index=False))
        print(f"\n任务完成，共找出 {len(result_df)} 只 {N_CONSECUTIVE_DAYS} 连板的股票。")

    print("==============================================")


if __name__ == "__main__":
    main()