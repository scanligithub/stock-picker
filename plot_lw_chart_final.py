import pandas as pd
import os
from lightweight_charts import Chart
import asyncio

MASTER_DATA_FILE = 'master_stock_data.feather'
STOCK_POOL_FILE = 'stock_pool.csv'

async def plot_final_humble_chart(stock_code, days_to_plot=250):
    if not os.path.exists(MASTER_DATA_FILE): print("错误: 缺少数据文件。"); return

    hist_data_full = pd.read_feather(MASTER_DATA_FILE)
    stock_pool = pd.read_csv(STOCK_POOL_FILE)
    stock_data = hist_data_full[hist_data_full['代码'] == stock_code].copy()
    if stock_data.empty: print(f"错误: 找不到股票 {stock_code}。"); return

    stock_data = stock_data.tail(days_to_plot + 30).reset_index(drop=True)
    stock_name = stock_pool[stock_pool['ts_code'] == stock_code]['name'].iloc[0]
    
    df_chart = stock_data.copy()
    df_chart.rename(columns={'日期':'time', '开盘':'open', '最高':'high', '最低':'low', '收盘':'close', '成交量':'volume'}, inplace=True)
    df_chart['time'] = pd.to_datetime(df_chart['time'])
    df_chart = df_chart.resample('W', on='time').last().reset_index()
    df_chart['time'] = df_chart['time'].dt.strftime('%Y-%m-%d')
    
    chart = Chart(width=1400, height=800)
    chart.set(df_chart)
    
    chart.layout(background_color='#FFFFFF', text_color='#131722')
    chart.watermark(f"{stock_name} ({stock_code})", color='rgba(0, 0, 0, 0.2)', font_size=48)
    chart.volume_config(scale_margin_top=0.9, scale_margin_bottom=0)

    colors = ['#FFA500', '#2196F3', '#FFC0CB', '#9C27B0']
    for i, period in enumerate([5, 10, 20, 30]):
        # 【最终核心修正】
        line_name = f'MA {period}'
        
        # 1. 计算均线
        df_chart[line_name] = df_chart['close'].rolling(window=period).mean()
        
        # 2. 准备专门用于这条线的数据DataFrame
        #    它的列名必须是 'time' 和 这条线的名字 (line_name)
        line_data = df_chart[['time', line_name]].copy()
        
        # 3. 删除NaN值
        line_data.dropna(inplace=True)
        
        # 4. 创建线对象并设置数据
        line = chart.create_line(line_name, color=colors[i], price_line=False, width=2)
        line.set(line_data)

    await chart.show_async(open_browser=True)


if __name__ == "__main__":
    target_stock = '000514.SZ'
    try:
        asyncio.run(plot_final_humble_chart(target_stock))
    except KeyboardInterrupt:
        print("\n--- 图表服务已关闭。 ---")