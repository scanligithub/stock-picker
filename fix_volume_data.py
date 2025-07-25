# fix_volume_data.py
import pandas as pd
import datetime

# --- 配置 ---
MASTER_DATA_FILE = 'master_stock_data.feather'
# 将这里的日期改为实际出错的日期
ERROR_DATE = pd.to_datetime('2025-07-24').normalize() 

def fix_volume_data():
    """
    一次性修复母版数据文件中，指定日期所有股票被错误处理的成交量数据。
    """
    try:
        df = pd.read_feather(MASTER_DATA_FILE)
        df['日期'] = pd.to_datetime(df['日期'])
        print(f"母版数据文件 '{MASTER_DATA_FILE}' 加载成功。")
    except Exception as e:
        print(f"读取文件时发生错误: {e}")
        return

    # 定位到需要修复的所有行 (只根据日期)
    target_rows_mask = (df['日期'] == ERROR_DATE)
    
    if target_rows_mask.any():
        num_rows = target_rows_mask.sum()
        print(f"找到了 {num_rows} 条在 {ERROR_DATE.date()} 的数据，准备修正成交量...")
        
        print("\n修正前数据示例:")
        print(df.loc[target_rows_mask, ['代码', '日期', '成交量']].head())
        
        # 将这些行的成交量乘以 100 来恢复
        df.loc[target_rows_mask, '成交量'] = df.loc[target_rows_mask, '成交量'] * 100
        
        print("\n数据已在内存中修正。")
        
        print("\n修正后数据示例:")
        print(df.loc[target_rows_mask, ['代码', '日期', '成交量']].head())
        
        try:
            df.reset_index(drop=True).to_feather(MASTER_DATA_FILE)
            print(f"\n数据修正并保存成功！'{MASTER_DATA_FILE}' 已被覆盖。")
        except Exception as e:
            print(f"\n保存文件时发生错误: {e}")
            
    else:
        print(f"未在文件中找到 {ERROR_DATE.date()} 的数据行，无需修正。")

if __name__ == "__main__":
    fix_volume_data()