# gui/kline_plot.py

import pandas as pd

def create_kline_plot(df):
    df['日期'] = pd.to_datetime(df['日期'])
    
    # 转换数据格式以适配KLineCharts
    kline_data = []
    for _, row in df.iterrows():
        kline_data.append({
            'timestamp': int(row['日期'].timestamp() * 1000),  # 转换为毫秒时间戳
            'open': row['开盘'] if '开盘' in row else row['收盘'],
            'high': row['最高'] if '最高' in row else row['收盘'],
            'low': row['最低'] if '最低' in row else row['收盘'],
            'close': row['收盘'],
            'volume': row['成交量'] if '成交量' in row else 0
        })
    
    # 生成KLineCharts的HTML内容
    klinechart_cdn_url = 'https://unpkg.com/klinecharts/dist/klinecharts.min.js'
    
    # 将数据转换为JavaScript数组格式
    kline_data_js = str(kline_data).replace("'", "\"")
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>价格走势和涨跌幅</title>
        <style>
            #kline-chart-container {{
                width: 100%;
                height: 600px;
            }}
        </style>
    </head>
    <body style="margin:0;padding:0;overflow:hidden;">
        <div id="kline-chart-container"></div>
        <script src="{klinechart_cdn_url}"></script>
        <script>
            // 初始化KLineChart
            const chart = klinecharts.init('kline-chart-container');
            
            // 设置样式选项，包括标题
            chart.setStyleOptions({{
                header: {{
                    title: {{
                        text: {{
                            content: '价格走势和涨跌幅',
                            color: '#333333',
                            size: 18,
                            family: 'Helvetica Neue',
                            weight: 'bold'
                        }}
                    }}
                }}
            }});
            
            // 设置数据
            chart.applyNewData({kline_data_js});
            
            // 添加收盘价指标
            chart.createIndicator('MA5', false, {{ id: 'price_pane' }});
            
            // 添加成交量指标
            chart.createIndicator('VOL', false, {{ id: 'volume_pane' }});
        </script>
    </body>
    </html>
    """
    
    return html_content