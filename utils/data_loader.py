# utils/data_loader.py

import pandas as pd
import os
from datetime import datetime, timedelta
import akshare as ak


def load_clean_hist_data(file_path=None):
    """加载并清洗历史行情数据"""
    from config import MASTER_DATA_FILE
    file_path = file_path or MASTER_DATA_FILE

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"找不到母版数据文件 {file_path}")
    df = pd.read_feather(file_path)
    df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
    df['代码'] = df['代码'].astype(str).apply(
        lambda x: x if '.' in x else (f"{x}.SZ" if x.startswith(('0','3')) else f"{x}.SH")
    )
    df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce')
    return df


def get_clean_snapshot_data(cache_file=None, force_refresh=False, max_retries=3):
    """获取并清洗实时快照行情（带缓存机制）"""
    from config import SNAPSHOT_FILE
    cache_file = cache_file or SNAPSHOT_FILE

    now = datetime.now()
    today = pd.to_datetime(now.date())
    is_market_closed = now.hour >= 15 and now.minute >= 5
    cache_duration_hours = 12 if is_market_closed else 2 / 60  # 盘中缓存2分钟

    if not force_refresh and os.path.exists(cache_file):
        try:
            df = pd.read_feather(cache_file)
            cache_time = pd.to_datetime(df['日期'].iloc[0])
            if (now - cache_time).total_seconds() / 3600 < cache_duration_hours:
                print(f"✅ 使用缓存快照数据（时间：{cache_time}）")
                return df[['代码', '日期', '收盘', '涨跌幅']]
        except Exception as e:
            print(f"⚠️ 缓存读取失败: {e}")

    # 开始请求新快照
    attempt = 0
    while attempt < max_retries:
        try:
            print("🔁 正在获取实时行情快照...")
            df = ak.stock_zh_a_spot_em()
            if df.empty:
                raise Exception("空数据")

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

            df = df[list(actual_columns.keys())].copy()
            df.rename(columns=actual_columns, inplace=True)

            df['日期'] = pd.to_datetime(datetime.now().date())
            df['代码'] = df['代码'].astype(str).apply(lambda x: f"{x}.SZ" if x.startswith(('0','3')) else f"{x}.SH")

            df = df[['代码', '日期', '收盘', '涨跌幅']].copy()
            df.to_feather(cache_file)
            print(f"💾 快照已缓存到 {cache_file}")
            return df

        except Exception as e:
            print(f"❌ 第 {attempt+1} 次获取失败: {e}")
            time.sleep(min(MIN_INTERVAL * (2 ** attempt), MAX_INTERVAL))
            attempt += 1

    print("🚫 达到最大重试次数，跳过本次快照获取")
    return None