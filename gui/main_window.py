import sys
import os
import pathlib
import pandas as pd
from PyQt5.QtWidgets import (QApplication, QMainWindow, QListWidget, QHBoxLayout,
                             QVBoxLayout, QWidget, QPushButton, QProgressBar, QTextEdit, QLabel,
                             QComboBox)
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtCore import Qt, QProcess, QTimer
from plotly.subplots import make_subplots
import plotly.graph_objects as go


def debug_print(*args):
    """统一调试输出"""
    sys.stdout.write("[DEBUG] " + " ".join(map(str, args)) + "\n")


# --- 输出重定向类 ---
class StreamRedirector:
    def __init__(self, callback):
        self.callback = callback

    def write(self, text):
        if text.strip():
            if isinstance(text, bytes):
                try:
                    text = text.decode('utf-8')
                except UnicodeDecodeError:
                    text = text.decode('gbk', errors='replace')
            text = text.strip()
            QTimer.singleShot(0, lambda: self.callback(text + '\n'))

    def flush(self):
        pass


# --- 子进程执行器 ---
class ExternalScriptWorker:
    def __init__(self, script_path, working_dir, args=None):
        self.script_path = script_path
        self.working_dir = working_dir
        self.args = args or []
        self.process = QProcess()
        self.process.setWorkingDirectory(working_dir)
        self.process.readyReadStandardOutput.connect(self._on_stdout_ready)
        self.process.finished.connect(self._on_finished)
        self.progress_updated = None
        self.finished = None

    def _on_stdout_ready(self):
        data = self.process.readAllStandardOutput().data()
        try:
            text = data.decode('utf-8')
        except UnicodeDecodeError:
            text = data.decode('gbk', errors='replace')
        lines = text.strip().split('\n')
        for line in lines:
            if line.startswith("PROGRESS:"):
                try:
                    progress_val = int(line.split(":")[1].strip())
                    if self.progress_updated:
                        self.progress_updated(progress_val)
                except (ValueError, IndexError):
                    pass
            sys.stdout.write(line + "\n")

    def _on_finished(self):
        exit_code = self.process.exitCode()
        output = self.process.readAllStandardOutput().data().decode('utf-8', errors='replace')
        error_output = self.process.readAllStandardError().data().decode('utf-8', errors='replace')
        debug_print("任务完成，退出码:", exit_code)
        if self.finished:
            if exit_code == 0:
                self.finished("任务成功完成！")
            else:
                self.finished(f"脚本执行出错，返回码: {exit_code}\n{error_output}\n{output}")

    def run(self):
        debug_print("启动 QProcess:", self.script_path)
        self.process.start(sys.executable, [self.script_path] + self.args)
        if not self.process.waitForStarted(5000):
            debug_print("启动脚本失败:", self.process.errorString())
            if self.finished:
                self.finished("启动脚本失败: " + self.process.errorString())

    def stop(self):
        debug_print("停止 QProcess")
        if self.process.state() == QProcess.Running:
            self.process.terminate()
            self.process.waitForFinished(2000)
            if self.process.state() == QProcess.Running:
                self.process.kill()


# --- 数据加载函数 ---
try:
    from utils.data_loader import load_clean_hist_data, get_clean_snapshot_data
except ImportError:
    def load_clean_hist_data(): return pd.DataFrame()
    def get_clean_snapshot_data(): return pd.DataFrame()


def load_combined_data(stock_code):
    debug_print(f"加载股票 {stock_code} 的数据")
    try:
        import os
        cache_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'snapshot_cache.csv')
        if os.path.exists(cache_path):
            df_snap = pd.read_csv(cache_path, parse_dates=['日期'])
            debug_print(f"使用缓存快照数据: {cache_path}")
        else:
            df_snap = get_clean_snapshot_data()
        df_hist = load_clean_hist_data()
        debug_print(f"历史数据: {len(df_hist)} 行，快照数据: {len(df_snap)} 行")

        def to_tushare_format(code):
            if pd.isna(code) or '.' in str(code): return code
            try:
                code_str = str(int(float(code)))
            except (ValueError, TypeError):
                return code
            if len(code_str) < 6: code_str = code_str.zfill(6)
            if code_str.startswith(('0', '3')):
                return f"{code_str}.SZ"
            if code_str.startswith(('6', '8', '9')):
                return f"{code_str}.SH"
            return code_str

        all_dfs = []
        if df_hist is not None and not df_hist.empty and '代码' in df_hist.columns:
            df_hist['代码'] = df_hist['代码'].apply(to_tushare_format)
            df_hist_stock = df_hist[df_hist['代码'] == stock_code].copy()
            if not df_hist_stock.empty:
                df_hist_stock['日期'] = pd.to_datetime(df_hist_stock['日期']).dt.normalize()
                today = pd.to_datetime('today').normalize()
                
                debug_print(f"今天是: {today.strftime('%Y-%m-%d')}")

                df_hist_stock = df_hist_stock[df_hist_stock['日期'] < today]
                df_hist_stock = df_hist_stock[df_hist_stock['日期'].dt.weekday < 5]
                all_dfs.append(df_hist_stock)

        if df_snap is not None and not df_snap.empty and '代码' in df_snap.columns:
            rename_map = {'最新价': '收盘', '今开': '开盘', '最高': '最高', '最低': '最低', '成交量': '成交量', '成交额': '成交额',
                          '换手率': '换手率', '涨跌幅': '涨跌幅'}
            df_snap.rename(columns=rename_map, inplace=True, errors='ignore')
            df_snap['代码'] = df_snap['代码'].apply(to_tushare_format)
            df_snap_stock = df_snap[df_snap['代码'] == stock_code].copy()
            if not df_snap_stock.empty:
                df_snap_stock['日期'] = pd.to_datetime(df_snap_stock['日期']).dt.normalize()
                df_snap_stock = df_snap_stock[df_snap_stock['日期'].dt.weekday < 5]
                for col in ['开盘', '最高', '最低']:
                    if col not in df_snap_stock.columns:
                        df_snap_stock[col] = df_snap_stock['收盘']
                all_dfs.append(df_snap_stock)

        if not all_dfs:
            return pd.DataFrame()

        combined = pd.concat(all_dfs, ignore_index=True)
        numeric_cols = ['开盘', '最高', '最低', '收盘', '成交量']
        for col in numeric_cols:
            if col in combined.columns:
                combined[col] = pd.to_numeric(combined[col], errors='coerce')
        combined.sort_values(by='日期', ascending=True, inplace=True)
        combined.drop_duplicates(subset=['日期'], keep='last', inplace=True)
        combined.dropna(subset=['成交量'], inplace=True)
        combined = combined[combined['成交量'] > 0].copy()
        combined[['开盘', '最高', '最低', '收盘']] = combined[['开盘', '最高', '最低', '收盘']].ffill()
        combined.dropna(subset=['开盘', '最高', '最低', '收盘'], inplace=True)
        combined.reset_index(drop=True, inplace=True)
        
        if not combined.empty and '日期' in combined.columns:
            combined['日期'] = pd.to_datetime(combined['日期'])
            today = pd.to_datetime('today').normalize()
            six_months_ago = today - pd.DateOffset(months=6)
            combined = combined[combined['日期'] >= six_months_ago].copy()
        
        ### --- START OF NEW DEBUG BLOCK --- ###
        if not combined.empty and stock_code == '000001.SZ':
             debug_print(f"--- 股票 {stock_code} 的最后3天合并数据 ---")
             debug_print(combined.tail(3).to_string())
             debug_print("------------------------------------")
        ### --- END OF NEW DEBUG BLOCK --- ###

        return combined

    except Exception as e:
        debug_print("load_combined_data 出错:", e)
        return pd.DataFrame()


class StockKLineViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        debug_print("StockKLineViewer 初始化")
        self.setWindowTitle("股票K线查看器 - 最终版")
        self.resize(1400, 800)
        self.update_process = None
        self.select_process = None
        self.update_progress_bar = None
        self.select_progress_bar = None
        self.log_window = None
        self.browser = None
        self.stock_list = None
        self.stock_pool = self._load_stock_pool()
        self.init_ui()
        self._setup_stdout_redirect()

    def _load_stock_pool(self, filename='stock_pool.csv'):
        try:
            df = pd.read_csv(filename)
            if '名称' in df.columns:
                df.rename(columns={'名称': 'name'}, inplace=True)
            df['ts_code'] = df['ts_code'].astype(str).apply(
                lambda x: x.strip() if isinstance(x, str) else x).apply(
                lambda x: x if '.' in str(x) else (
                    f"{str(x).zfill(6)}.SZ" if str(x).startswith(('0', '3')) else f"{str(x).zfill(6)}.SH"))
            return df
        except FileNotFoundError:
            return pd.DataFrame(columns=['ts_code', 'name'])

    def _setup_stdout_redirect(self):
        self.stdout_redirector = StreamRedirector(self._update_log_window)
        self.stderr_redirector = StreamRedirector(self._update_log_window)
        sys.stdout = self.stdout_redirector
        sys.stderr = self.stderr_redirector

    def _update_log_window(self, text):
        """优化日志窗口更新"""
        cursor = self.log_window.textCursor()
        cursor.movePosition(cursor.End)
        cursor.insertText(text)
        self.log_window.setTextCursor(cursor)
        self.log_window.ensureCursorVisible()

    def init_ui(self):
        debug_print("初始化界面")
        left_panel_layout = QVBoxLayout()
        self.update_button = QPushButton("当日盘后更新")
        self.update_button.clicked.connect(self.run_update_script)
        left_panel_layout.addWidget(self.update_button)
        self.update_progress_bar = QProgressBar()
        self.update_progress_bar.setRange(0, 100)
        self.update_progress_bar.setVisible(False)
        self.update_progress_bar.setFormat("更新进度条")
        left_panel_layout.addWidget(self.update_progress_bar)
        # 策略选择下拉框
        self.strategy_combo = QComboBox()
        self.strategy_combo.addItems(["高价股筛选", "均线金叉", "N连板"])
        left_panel_layout.addWidget(self.strategy_combo)
        
        self.select_button = QPushButton("选股")
        self.select_button.clicked.connect(self.run_select_script)
        left_panel_layout.addWidget(self.select_button)
        
        # 创建包含文本和标签的水平布局
        select_count_layout = QHBoxLayout()
        select_count_layout.addWidget(QLabel("已经选出的股票数量："))
        self.select_count_label = QLabel("0")
        select_count_layout.addWidget(self.select_count_label)
        select_count_layout.addStretch()  # 添加弹性空间，将控件推到左侧
        left_panel_layout.addLayout(select_count_layout)
        self.select_progress_bar = QProgressBar()
        self.select_progress_bar.setRange(0, 100)
        self.select_progress_bar.setVisible(False)
        self.select_progress_bar.setFormat("选股进度条")
        left_panel_layout.addWidget(self.select_progress_bar)
        self.stock_list = QListWidget()
        self.stock_list.setMinimumWidth(300)
        self.stock_list.setMinimumHeight(300)
        self.stock_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.stock_list.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        if self.stock_pool.empty:
            self.stock_list.addItem("无可用股票数据")
        else:
            items = [f"{row['ts_code']} - {row['name']}" for _, row in self.stock_pool.iterrows()]
            self.stock_list.addItems(items)
        self.stock_list.currentItemChanged.connect(self.on_stock_selected)
        left_panel_layout.addWidget(self.stock_list)
        left_panel_layout.addStretch(1)
        log_label = QLabel("更新和选股运行时打印信息:")
        left_panel_layout.addWidget(log_label)
        self.log_window = QTextEdit()
        self.log_window.setReadOnly(True)
        self.log_window.setFixedHeight(150)
        left_panel_layout.addWidget(self.log_window)
        left_panel = QWidget()
        left_panel.setLayout(left_panel_layout)
        left_panel.setMaximumWidth(300)
        self.browser = QWebEngineView()
        main_layout = QHBoxLayout()
        main_layout.addWidget(left_panel)
        main_layout.addWidget(self.browser, 1)
        container = QWidget()
        container.setLayout(main_layout)
        self.setCentralWidget(container)
        if not self.stock_pool.empty and self.stock_list.count() > 0:
            self.stock_list.setCurrentRow(0)

    def run_update_script(self):
        debug_print("点击【当日盘后更新】按钮")
        if self.update_process or self.select_process:
            print("已有任务在运行中，请稍后再试。")
            return
        self.log_window.clear()
        self.update_progress_bar.setVisible(True)
        self.update_progress_bar.setValue(0)
        self.update_progress_bar.setStyleSheet("")
        self.update_button.setEnabled(False)
        self.select_button.setEnabled(False)
        project_root = str(pathlib.Path(__file__).parent.parent)
        script_path = os.path.join(project_root, "2_update_daily_data_fully_auto.py")
        debug_print("启动更新脚本:", script_path)
        self.update_process = ExternalScriptWorker(script_path, project_root)
        self.update_process.progress_updated = lambda val: self.update_progress_bar.setValue(val)
        self.update_process.finished = lambda msg: self.on_script_finished(msg, 'update')
        self.update_process.run()

    def run_select_script(self):
        debug_print("选股按钮被点击，开始执行选股流程")
        if self.update_process or self.select_process:
            print("已有任务在运行中，请稍后再试。")
            return
        self.log_window.clear()
        self.select_progress_bar.setVisible(True)
        self.select_progress_bar.setValue(0)
        self.select_progress_bar.setStyleSheet("")
        self.update_button.setEnabled(False)
        self.select_button.setEnabled(False)
        project_root = str(pathlib.Path(__file__).parent.parent)
        script_path = os.path.join(project_root, "3_stock_selector.py")
        selected_strategy = self.strategy_combo.currentText()
        self.select_process = ExternalScriptWorker(script_path, project_root, [selected_strategy])
        self.select_process.progress_updated = lambda val: self.select_progress_bar.setValue(val)
        self.select_process.finished = lambda msg: self.on_script_finished(msg, 'select')
        self.select_process.run()

    def on_script_finished(self, message, task_type):
        print(message)
        self.update_button.setEnabled(True)
        self.select_button.setEnabled(True)
        if task_type == 'update':
            self.update_process = None
            self.update_progress_bar.setFormat(message.split('\n')[0])
            if "出错" in message:
                self.update_progress_bar.setStyleSheet("QProgressBar::chunk { background-color: red; }")
            else:
                self.update_progress_bar.setValue(100)
        else:
            self.select_process = None
            self.select_progress_bar.setFormat(message.split('\n')[0])
            if "出错" in message:
                self.select_progress_bar.setStyleSheet("QProgressBar::chunk { background-color: red; }")
            else:
                self.select_progress_bar.setValue(100)
                try:
                    self.stock_pool = self._load_stock_pool('selected_stocks.csv')
                    count = len(self.stock_pool)
                    self.select_count_label.setText(str(count))
                    self.stock_list.clear()
                    if self.stock_pool.empty:
                        self.stock_list.addItem("无选股结果")
                    else:
                        self.stock_list.addItems([f"{row['ts_code']} - {row['name']}" for _, row in self.stock_pool.iterrows()])
                    if not self.stock_pool.empty and self.stock_list.count() > 0:
                        self.stock_list.setCurrentRow(0)
                except Exception as e:
                    debug_print(f"更新选股结果失败: {str(e)}")
                    self.stock_list.clear()
                    self.stock_list.addItem("选股结果加载失败")

    def closeEvent(self, event):
        debug_print("窗口关闭事件触发")
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        if self.update_process:
            self.update_process.stop()
        if self.select_process:
            self.select_process.stop()
        event.accept()

    def on_stock_selected(self, current, previous):
        if not current:
            return
        
        debug_print(f"股票选择: {current.text()}")
        if " - " in current.text():
            stock_code = current.text().split(" - ")[0]
            try:
                df = load_combined_data(stock_code)
                if df.empty or len(df) < 30:
                    self.browser.setHtml(
                        f"<html><body style='display:flex;justify-content:center;align-items:center;height:100vh;'><h1 style='color:red;'>股票 {stock_code} 最近6个月数据不足30天</h1></body></html>")
                    return
                
                fig = self.plot_kline(df, stock_code)
                if fig:
                    self.show_plotly_figure(fig)
                
            except Exception as e:
                debug_print("加载、绘制或显示K线图时出错:", e)
                import traceback
                debug_print(traceback.format_exc())
                self.browser.setHtml(f"<html><body><h1>操作失败:</h1><pre>{e}</pre></body></html>")

    def plot_kline(self, df, stock_code):
        debug_print(f"绘制 K 线图: {stock_code}")
        plot_df = df.copy()
        plot_df['日期'] = pd.to_datetime(plot_df['日期'])

        price_ma_periods, volume_ma_periods = [5, 10, 20, 30], [5, 10, 20]
        for p in price_ma_periods:
            plot_df[f'MA{p}'] = plot_df['收盘'].rolling(window=p).mean()
        for p in volume_ma_periods:
            plot_df[f'VOL_MA{p}'] = plot_df['成交量'].rolling(window=p).mean()
        
        plot_df['涨跌幅'] = (plot_df['收盘'] - plot_df['收盘'].shift(1)) / plot_df['收盘'].shift(1)
        
        plot_df['日期字符串'] = plot_df['日期'].dt.strftime('%Y-%m-%d')
        
        if plot_df.empty:
            raise ValueError("处理数据后为空")

        kline_height_ratio, volume_height_ratio = 2 / 3, 1 / 3
        price_cols = ['开盘', '最高', '最低', '收盘'] + [f'MA{p}' for p in price_ma_periods]
        vol_cols = ['成交量'] + [f'VOL_MA{p}' for p in volume_ma_periods]

        price_min_val = plot_df[price_cols].min(skipna=True).min(skipna=True)
        price_max_val = plot_df[price_cols].max(skipna=True).max(skipna=True)
        price_range = price_max_val - price_min_val if price_max_val > price_min_val else 1

        vol_max = plot_df[vol_cols].max(skipna=True).max(skipna=True)
        if pd.isna(vol_max) or vol_max == 0:
            vol_max = 1

        offset = volume_height_ratio
        for col in price_cols:
            plot_df[f'{col}_norm'] = ((plot_df[col] - price_min_val) / price_range * kline_height_ratio) + offset
        for col in vol_cols:
            plot_df[f'{col}_norm'] = (plot_df[col] / vol_max * volume_height_ratio)

        fig = go.Figure()

        fig.add_trace(go.Candlestick(
            x=plot_df['日期字符串'],
            open=plot_df['开盘_norm'], high=plot_df['最高_norm'], low=plot_df['最低_norm'], close=plot_df['收盘_norm'],
            name='K线',
            increasing_line_color='red', decreasing_line_color='green',
            hoverinfo='none'
        ))

        fig.add_trace(go.Scatter(
            x=plot_df['日期字符串'],
            y=plot_df['收盘_norm'],
            mode='markers',
            marker=dict(color='rgba(0,0,0,0)'),
            name='价格',
            showlegend=False,
            customdata=plot_df[['开盘', '最高', '最低', '收盘', '涨跌幅']].values,
            hovertemplate=(
                '<b>涨跌</b>: %{customdata[4]:.2%}<br>' +
                '<b>开盘</b>: %{customdata[0]:.2f}<br>' +
                '<b>最高</b>: %{customdata[1]:.2f}<br>' +
                '<b>最低</b>: %{customdata[2]:.2f}<br>' +
                '<b>收盘</b>: %{customdata[3]:.2f}<extra></extra>'
            )
        ))
        
        for p in price_ma_periods:
            fig.add_trace(go.Scatter(
                x=plot_df['日期字符串'], y=plot_df[f'MA{p}_norm'], name=f'MA{p}', mode='lines', line=dict(width=1),
                customdata=plot_df[f'MA{p}'],
                hovertemplate=f'MA{p}: %{{customdata:.2f}}<extra></extra>'
            ))

        colors = ['red' if r['收盘'] >= r['开盘'] else 'green' for _, r in plot_df.iterrows()]
        fig.add_trace(go.Bar(
                x=plot_df['日期字符串'], y=plot_df['成交量_norm'], name='成交量 (股)', marker_color=colors,
            customdata=plot_df['成交量'],
            hovertemplate='成交量: %{customdata:,.0f} (股)<extra></extra>'
        ))

        for p in volume_ma_periods:
            fig.add_trace(go.Scatter(
                x=plot_df['日期字符串'], y=plot_df[f'VOL_MA{p}_norm'], name=f'VOL_MA{p}', mode='lines', line=dict(width=1),
                customdata=plot_df[f'VOL_MA{p}'],
                hovertemplate=f'VOL_MA{p}: %{{customdata:,.0f}}<extra></extra>'
            ))

        valid_df = plot_df.dropna(subset=['最高', '最低'])
        if not valid_df.empty:
            highest_point = valid_df.loc[valid_df['最高'].idxmax()]
            lowest_point = valid_df.loc[valid_df['最低'].idxmin()]
            fig.add_annotation(x=highest_point['日期字符串'], y=highest_point['最高_norm'], text=f"H: {highest_point['最高']:.2f}",
                               showarrow=True, arrowhead=4, ax=-40, ay=-40, font=dict(color="purple"), arrowcolor="purple")
            fig.add_annotation(x=lowest_point['日期字符串'], y=lowest_point['最低_norm'], text=f"L: {lowest_point['最低']:.2f}",
                               showarrow=True, arrowhead=4, ax=40, ay=40, font=dict(color="blue"), arrowcolor="blue")

        stock_name_series = self.stock_pool[self.stock_pool['ts_code'] == stock_code]['name']
        stock_name = stock_name_series.iloc[0] if not stock_name_series.empty else stock_code

        fig.update_layout(
            title_text=f"{stock_name} ({stock_code})",
            xaxis_rangeslider_visible=False,
            height=750,
            margin=dict(l=20, r=20, t=50, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.2, xanchor="right", x=1),
            xaxis=dict(
                type='category',
            ),
            yaxis=dict(showticklabels=False, showgrid=True, zeroline=False, range=[0, 1.0], fixedrange=False)
        )
        return fig

    def show_plotly_figure(self, fig):
        debug_print("显示 K 线图")
        fig.update_xaxes(showspikes=True, spikemode='across', spikesnap='cursor', spikedash='dot', spikecolor='grey', spikethickness=1)
        fig.update_yaxes(showspikes=True, spikemode='across', spikesnap='cursor', spikedash='dot', spikecolor='grey', spikethickness=1)
        fig.update_layout(hovermode='x unified', spikedistance=-1, hoverdistance=-1, dragmode='pan')

        cdn_url = 'file:///D:/python/stock-picker/js/plotly.min.js'
        raw_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
        full_html = f"""
        <html><head><meta charset="utf-8"><script src="{cdn_url}"></script></head>
        <body style="margin:0;padding:0;overflow:hidden;">{raw_html}</body></html>
        """
        self.browser.setHtml(full_html)


def handle_exception(exc_type, exc_value, exc_traceback):
    debug_print("全局未处理异常:")
    import traceback
    traceback.print_exception(exc_type, exc_value, exc_traceback)
    sys.exit(1)


sys.excepthook = handle_exception


if __name__ == '__main__':
    debug_print("应用程序启动")
    QApplication.setAttribute(Qt.AA_EnableHighDpiScaling)
    app = QApplication(sys.argv)
    viewer = StockKLineViewer()
    viewer.show()
    sys.exit(app.exec_())