# gui/kline_plot.py

from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd

def create_kline_plot(df):
    df['日期'] = pd.to_datetime(df['日期'])
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        subplot_titles=('价格走势', '涨跌幅'),
                        vertical_spacing=0.05)

    fig.add_trace(go.Scatter(
        x=df['日期'], 
        y=df['收盘'], 
        name='收盘价',
        hovertemplate='收盘价: %{y}<extra></extra>'
    ), row=1, col=1)
    fig.add_trace(go.Bar(
        x=df['日期'], 
        y=df['涨跌幅'], 
        name='涨跌幅',
        hovertemplate='涨跌幅: %{y}%<extra></extra>'
    ), row=2, col=1)

    fig.update_layout(
        xaxis_rangeslider_visible=False,
        height=600,
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    fig.update_xaxes(dtick="W", tickformat="%Y-%m-%d")

    return fig