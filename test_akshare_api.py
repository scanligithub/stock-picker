import akshare as ak
import pandas as pd
from datetime import datetime

# 测试Akshare接口基本功能
def test_akshare_api():
    print("--- 开始Akshare接口测试 --- ")
    print(f"Akshare版本: {ak.__version__}")
    
    # 测试1: 获取股票列表
    try:
        stock_list = ak.stock_zh_a_spot_em()
        print(f"测试1: 获取股票列表成功，共{len(stock_list)}只股票")
        print(f"股票列表样例: {stock_list['代码'].head(5).tolist()}")
    except Exception as e:
        print(f"测试1: 获取股票列表失败: {str(e)}")
    
    # 测试2: 获取单个股票历史数据
    try:
        stock_code = "600519"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        
        print(f"测试2: 获取股票 {stock_code} {start_date}至{end_date} 数据...")
        df = ak.stock_zh_a_hist_tx(
            symbol=stock_code,
            start_date=start_date,
            end_date=end_date
        )
        
        print(f"测试2: 返回数据行数: {len(df)}")
        if not df.empty:
            print(f"数据样例:\n{df[['日期', '开盘', '收盘', '最高', '最低']].head()}")
        else:
            print("测试2: 未获取到任何数据")
    except Exception as e:
        print(f"测试2: 获取历史数据失败: {str(e)}")
    
    # 测试3: 尝试不同的股票代码格式
    try:
        stock_code = "sh600519"
        print(f"测试3: 使用带市场前缀的股票代码 {stock_code}...")
        df = ak.stock_zh_a_hist_tx(
            symbol=stock_code,
            start_date=start_date,
            end_date=end_date
        )
        print(f"测试3: 返回数据行数: {len(df)}")
    except Exception as e:
        print(f"测试3: 获取数据失败: {str(e)}")
    
    # 测试4: 尝试获取指数数据
    try:
        print(f"测试4: 获取上证指数数据...")
        df = ak.stock_zh_index_daily(symbol="sh000001")
        print(f"测试4: 返回数据行数: {len(df)}")
    except Exception as e:
        print(f"测试4: 获取指数数据失败: {str(e)}")

if __name__ == "__main__":
    test_akshare_api()