# utils/data_loader.py

import pandas as pd
import os
from datetime import datetime
import akshare as ak
from config import MIN_INTERVAL, MAX_INTERVAL
import time


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
    # 历史数据已为股单位，无需转换
    # if '成交量' in df.columns:
    #     df['成交量'] = df['成交量'] * 100
    return df


def get_clean_snapshot_data(cache_file=None, force_refresh=False, max_retries=3):
    """获取并清洗实时快照行情（带缓存机制）"""
    # 使用用户检查的CSV文件路径
    cache_file = cache_file or 'snapshot_cache.csv'
    # 强制刷新缓存以确保获取最新数据
    force_refresh = True  # [TEMP] 临时强制刷新缓存
    # 临时强制刷新缓存以获取最新数据并应用单位转换
    force_refresh = True

    now = datetime.now()
    today = pd.to_datetime(now)
    is_market_closed = now.hour >= 15 and now.minute >= 5
    cache_duration_minutes = 12 * 60 if is_market_closed else 2  # 缓存时间（分钟）

    # ✅ 强制刷新逻辑（调试时建议开启）
    if not force_refresh and os.path.exists(cache_file):
        try:
            df = pd.read_csv(cache_file)
            cache_time = pd.to_datetime(df['日期'].iloc[0])
            if (now - cache_time).total_seconds() / 60 < cache_duration_minutes:
                print(f"[SUCCESS] 使用缓存快照数据（时间：{cache_time}）")
                # 检查并转换缓存数据的成交量单位
                if '成交量' in df.columns:
                    median_volume = df['成交量'].median()
                    if median_volume < 1000 and median_volume > 0:
                        df['成交量'] = df['成交量'] * 100
                        print(f"[DEBUG] 缓存数据转换: {median_volume} → {median_volume*100} (手→股)")
                    else:
                        print(f"[DEBUG] 缓存成交量样本: {df['成交量'].iloc[0]}")
                return df[['代码', '日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '涨跌幅']]
        except Exception as e:
            print(f"[WARNING] 缓存读取失败: {e}")

    # 开始请求新快照
    attempt = 0
    while attempt < max_retries:
        try:
            print("[INFO] 正在获取实时行情快照...")
            df = ak.stock_zh_a_spot_em()
            print(f"[DEBUG] API返回的列: {df.columns.tolist()}")  # 调试API列名
            # 移动到日期设置后打印
            pass
            if df.empty:
                raise Exception("空数据")

            column_mapping = {
                '代码': ['代码', 'symbol'],
                '名称': ['名称', 'name'],
                '开盘': ['今开', '开盘', 'open'],
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

            # ✅ 设置精确到秒的时间戳
            # 根据当前时间设置正确的交易日期
            now = datetime.now()
            print(f"[DEBUG] 当前系统日期: {now.date()}")
            # 改进的交易日期判断逻辑
            current_hour = now.hour
            current_minute = now.minute
            is_weekday = now.weekday() < 5  # 周一至周五
            market_started = current_hour > 9 or (current_hour == 9 and current_minute >= 30)
            market_closed = current_hour >= 15

            if is_weekday:
                # 工作日无论是否交易时间均使用当天日期
                df['日期'] = pd.to_datetime(now.date())
            else:
                # 非交易时间/周末/节假日使用前一个交易日
                offset = 1
                while True:
                    prev_date = now - pd.Timedelta(days=offset)
                    if prev_date.weekday() < 5:  # 跳过周末
                        df['日期'] = pd.to_datetime(prev_date.date())
                        break
                    offset += 1
            print(f"[DEBUG] 生成的日期: {df['日期'].iloc[0]}")
            df['代码'] = df['代码'].astype(str).apply(lambda x: f"{x}.SZ" if x.startswith(('0','3')) else f"{x}.SH")

            # ✅ 将“手”转为“股”
            if '成交量' in df.columns:
                # 固定单位转换：手→股
                original_volume = df['成交量'].copy()
                df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce')
                df['成交量'] = df['成交量'] * 100  # 确保从手转换为股
                
                # 添加转换验证
                if not df['成交量'].isna().all():
                    print(f"[DEBUG] 成交量转换: {original_volume.iloc[0]} → {df['成交量'].iloc[0]}")
                else:
                    print("[WARNING] 成交量数据为空或无效")

            # ✅ 返回更多字段支持 K 线图绘制
            df = df[['代码', '日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '涨跌幅']].copy()
            df.to_csv(cache_file, index=False)
            print(f"[CACHE] 快照已缓存到 {cache_file}")
            # 打印缓存数据样本以验证日期
            print(f"[DEBUG] 缓存数据日期样本: {df['日期'].unique()[:5]}")
            return df

        except Exception as e:
            print(f"[ERROR] 第 {attempt+1} 次获取失败: {e}")
            time.sleep(min(MIN_INTERVAL * (2 ** attempt), MAX_INTERVAL))
            attempt += 1

    print("🚫 达到最大重试次数，跳过本次快照获取")
    # 尝试使用缓存数据
    if os.path.exists(cache_file):
        try:
            df = pd.read_csv(cache_file)
            print(f"[SUCCESS] 使用缓存快照数据")
            # 检查并转换缓存数据的成交量单位
            if '成交量' in df.columns:
                median_volume = df['成交量'].median()
                if median_volume < 1000 and median_volume > 0:
                    df['成交量'] = df['成交量'] * 100
                    print(f"[DEBUG] 缓存数据转换: {median_volume} → {median_volume*100} (手→股)")
                else:
                    print(f"[DEBUG] 缓存成交量样本: {df['成交量'].iloc[0]}")
            return df[['代码', '日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '涨跌幅']]
        except Exception as e:
            print(f"[ERROR] 缓存数据读取失败: {e}")
    return None