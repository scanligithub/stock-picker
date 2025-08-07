import sys
import os
import pathlib
import pandas as pd
import json # 引入json库
from PyQt5.QtWidgets import (QApplication, QMainWindow, QListWidget, QHBoxLayout,
                             QVBoxLayout, QWidget, QPushButton, QProgressBar, QTextEdit, QLabel)
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtCore import Qt, QProcess, QTimer, QUrl


def debug_print(*args):
    """统一调试输出"""
    sys.stdout.write("[DEBUG] " + " ".join(map(str, args)) + "\n")


# --- 输出重定向类 --- (代码不变)
class StreamRedirector:
    def __init__(self, callback): self.callback = callback
    def write(self, text):
        if text.strip():
            if isinstance(text, bytes):
                try: text = text.decode('utf-8')
                except UnicodeDecodeError: text = text.decode('gbk', errors='replace')
            text = text.strip()
            QTimer.singleShot(0, lambda: self.callback(text + '\n'))
    def flush(self): pass


# --- 子进程执行器 --- (代码不变)
class ExternalScriptWorker:
    def __init__(self, script_path, working_dir):
        self.script_path, self.working_dir, self.process = script_path, working_dir, QProcess()
        self.process.setWorkingDirectory(working_dir)
        self.process.readyReadStandardOutput.connect(self._on_stdout_ready)
        self.process.finished.connect(self._on_finished)
        self.progress_updated, self.finished = None, None
    def _on_stdout_ready(self):
        data = self.process.readAllStandardOutput().data()
        text = data.decode('utf-8', errors='replace')
        lines = text.strip().split('\n')
        for line in lines:
            if line.startswith("PROGRESS:"):
                try:
                    progress_val = int(line.split(":")[1].strip())
                    if self.progress_updated: self.progress_updated(progress_val)
                except (ValueError, IndexError): pass
            sys.stdout.write(line + "\n")
    def _on_finished(self):
        exit_code = self.process.exitCode()
        output = self.process.readAllStandardOutput().data().decode('utf-8', errors='replace')
        error_output = self.process.readAllStandardError().data().decode('utf-8', errors='replace')
        debug_print("任务完成，退出码:", exit_code)
        if self.finished:
            if exit_code == 0: self.finished("任务成功完成！")
            else: self.finished(f"脚本执行出错，返回码: {exit_code}\n{error_output}\n{output}")
    def run(self):
        debug_print("启动 QProcess:", self.script_path)
        self.process.start(sys.executable, [self.script_path])
        if not self.process.waitForStarted(5000):
            debug_print("启动脚本失败:", self.process.errorString())
            if self.finished: self.finished("启动脚本失败: " + self.process.errorString())
    def stop(self):
        if self.process.state() == QProcess.Running: self.process.terminate(); self.process.waitForFinished(2000)
        if self.process.state() == QProcess.Running: self.process.kill()


# --- 数据加载函数 --- (代码不变)
try:
    from utils.data_loader import load_clean_hist_data, get_clean_snapshot_data
except ImportError:
    def load_clean_hist_data(): return pd.DataFrame()
    def get_clean_snapshot_data(): return pd.DataFrame()

def load_combined_data(stock_code):
    debug_print(f"加载股票 {stock_code} 的数据")
    try:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        cache_path = os.path.join(project_root, 'snapshot_cache.csv')
        
        if os.path.exists(cache_path): df_snap = pd.read_csv(cache_path, parse_dates=['日期'])
        else: df_snap = get_clean_snapshot_data()
        df_hist = load_clean_hist_data()

        def to_tushare_format(code):
            if pd.isna(code) or '.' in str(code): return code
            try: code_str = str(int(float(code))).zfill(6)
            except (ValueError, TypeError): return code
            if code_str.startswith(('0', '3')): return f"{code_str}.SZ"
            if code_str.startswith(('6', '8', '9')): return f"{code_str}.SH"
            return code_str

        all_dfs = []
        if not df_hist.empty:
            df_hist['代码'] = df_hist['代码'].apply(to_tushare_format)
            df_hist_stock = df_hist[df_hist['代码'] == stock_code].copy()
            if not df_hist_stock.empty: all_dfs.append(df_hist_stock)
        
        if not df_snap.empty:
            rename_map = {'最新价':'收盘','今开':'开盘','最高':'最高','最低':'最低','成交量':'成交量'}
            df_snap.rename(columns=rename_map, inplace=True, errors='ignore')
            df_snap['代码'] = df_snap['代码'].apply(to_tushare_format)
            df_snap_stock = df_snap[df_snap['代码'] == stock_code].copy()
            if not df_snap_stock.empty: all_dfs.append(df_snap_stock)
            
        if not all_dfs: return pd.DataFrame()
        
        combined = pd.concat(all_dfs, ignore_index=True)
        combined['日期'] = pd.to_datetime(combined['日期']).dt.normalize()
        numeric_cols = ['开盘', '最高', '最低', '收盘', '成交量']
        for col in numeric_cols: combined[col] = pd.to_numeric(combined[col], errors='coerce')
        
        combined.sort_values(by='日期', ascending=True, inplace=True)
        combined.drop_duplicates(subset=['日期'], keep='last', inplace=True)
        combined.dropna(subset=['成交量'], inplace=True)
        combined = combined[combined['成交量'] > 0].copy()
        combined[numeric_cols] = combined[numeric_cols].ffill()
        combined.dropna(subset=numeric_cols, inplace=True)
        combined.reset_index(drop=True, inplace=True)
        
        if not combined.empty:
            six_months_ago = pd.to_datetime('today').normalize() - pd.DateOffset(months=6)
            combined = combined[combined['日期'] >= six_months_ago].copy()
            
        return combined
    except Exception as e:
        debug_print(f"load_combined_data 出错: {e}")
        return pd.DataFrame()


class StockKLineViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        debug_print("StockKLineViewer 初始化")
        self.setWindowTitle("股票K线查看器 - 最终版")
        self.resize(1400, 800)
        self.update_process, self.select_process = None, None
        self.update_progress_bar, self.select_progress_bar = None, None
        self.log_window, self.browser, self.stock_list = None, None, None
        
        self.project_root = str(pathlib.Path(__file__).parent.parent)
        
        self.stock_pool = self._load_stock_pool('stock_pool.csv')
        self.init_ui()
        self._setup_stdout_redirect()

    def _load_stock_pool(self, filename='stock_pool.csv'):
        file_path = os.path.join(self.project_root, filename)
        try:
            df = pd.read_csv(file_path)
            if '名称' in df.columns: df.rename(columns={'名称': 'name'}, inplace=True)
            df['ts_code'] = df['ts_code'].astype(str).apply(
                lambda x: x.strip() if isinstance(x, str) else x).apply(
                lambda x: x if '.' in str(x) else (
                    f"{str(x).zfill(6)}.SZ" if str(x).startswith(('0', '3')) else f"{str(x).zfill(6)}.SH"))
            return df
        except FileNotFoundError: return pd.DataFrame(columns=['ts_code', 'name'])

    def _setup_stdout_redirect(self):
        self.stdout_redirector = StreamRedirector(self._update_log_window)
        self.stderr_redirector = StreamRedirector(self._update_log_window)
        sys.stdout = self.stdout_redirector
        sys.stderr = self.stderr_redirector

    def _update_log_window(self, text):
        cursor = self.log_window.textCursor()
        cursor.movePosition(cursor.End); cursor.insertText(text)
        self.log_window.setTextCursor(cursor); self.log_window.ensureCursorVisible()

    def init_ui(self):
        debug_print("初始化界面")
        left_panel_layout = QVBoxLayout()
        self.update_button = QPushButton("当日盘后更新"); self.update_button.clicked.connect(self.run_update_script); left_panel_layout.addWidget(self.update_button)
        self.update_progress_bar = QProgressBar(); self.update_progress_bar.setRange(0, 100); self.update_progress_bar.setVisible(False); left_panel_layout.addWidget(self.update_progress_bar)
        self.select_button = QPushButton("选股"); self.select_button.clicked.connect(self.run_select_script); left_panel_layout.addWidget(self.select_button)
        select_count_layout = QHBoxLayout(); select_count_layout.addWidget(QLabel("已选股票数:")); self.select_count_label = QLabel("0"); select_count_layout.addWidget(self.select_count_label); select_count_layout.addStretch(); left_panel_layout.addLayout(select_count_layout)
        self.stock_list = QListWidget(); self.stock_list.setMinimumWidth(300)
        if self.stock_pool.empty: self.stock_list.addItem("无可用股票数据")
        else: self.stock_list.addItems([f"{row['ts_code']} - {row['name']}" for _, row in self.stock_pool.iterrows()])
        self.stock_list.currentItemChanged.connect(self.on_stock_selected); left_panel_layout.addWidget(self.stock_list)
        self.log_window = QTextEdit(); self.log_window.setReadOnly(True); self.log_window.setFixedHeight(150); left_panel_layout.addWidget(self.log_window)
        left_panel = QWidget(); left_panel.setLayout(left_panel_layout); left_panel.setMaximumWidth(300)
        self.browser = QWebEngineView()
        main_layout = QHBoxLayout(); main_layout.addWidget(left_panel); main_layout.addWidget(self.browser, 1)
        container = QWidget(); container.setLayout(main_layout); self.setCentralWidget(container)
        if not self.stock_pool.empty: self.stock_list.setCurrentRow(0)
    
    def run_update_script(self):
        debug_print("点击【当日盘后更新】按钮")
        if self.update_process or self.select_process: print("已有任务在运行中..."); return
        self.log_window.clear(); self.update_progress_bar.setVisible(True); self.update_progress_bar.setValue(0)
        self.update_button.setEnabled(False); self.select_button.setEnabled(False)
        script_path = os.path.join(self.project_root, "2_update_daily_data_fully_auto.py")
        self.update_process = ExternalScriptWorker(script_path, self.project_root)
        self.update_process.progress_updated = self.update_progress_bar.setValue
        self.update_process.finished = lambda msg: self.on_script_finished(msg, 'update')
        self.update_process.run()
    def run_select_script(self):
        debug_print("选股按钮被点击")
        if self.update_process or self.select_process: print("已有任务在运行中..."); return
        self.log_window.clear(); self.update_progress_bar.setVisible(True); self.update_progress_bar.setValue(0)
        self.update_button.setEnabled(False); self.select_button.setEnabled(False)
        script_path = os.path.join(self.project_root, "3_stock_selector.py")
        self.select_process = ExternalScriptWorker(script_path, self.project_root)
        self.select_process.progress_updated = self.update_progress_bar.setValue
        self.select_process.finished = lambda msg: self.on_script_finished(msg, 'select')
        self.select_process.run()
    def on_script_finished(self, message, task_type):
        print(message)
        self.update_button.setEnabled(True); self.select_button.setEnabled(True)
        progress_bar = self.update_progress_bar
        progress_bar.setFormat(message.split('\n')[0])
        if "出错" in message: progress_bar.setStyleSheet("QProgressBar::chunk { background-color: red; }")
        else: progress_bar.setValue(100)
        if task_type == 'update': self.update_process = None
        else:
            self.select_process = None
            self.stock_pool = self._load_stock_pool('selected_stocks.csv')
            self.select_count_label.setText(str(len(self.stock_pool)))
            self.stock_list.clear()
            if self.stock_pool.empty: self.stock_list.addItem("无选股结果")
            else: self.stock_list.addItems([f"{r['ts_code']} - {r['name']}" for _, r in self.stock_pool.iterrows()])
            if not self.stock_pool.empty: self.stock_list.setCurrentRow(0)
    def closeEvent(self, event):
        debug_print("窗口关闭事件触发")
        sys.stdout = sys.__stdout__; sys.stderr = sys.__stderr__
        if self.update_process: self.update_process.stop()
        if self.select_process: self.select_process.stop()
        event.accept()

    def on_stock_selected(self, current, previous):
        if not current: return
        debug_print(f"股票选择: {current.text()}")
        if " - " in current.text():
            stock_code = current.text().split(" - ")[0]
            try:
                df = load_combined_data(stock_code)
                if df.empty or len(df) < 30:
                    self.browser.setHtml(f"<html><body><h1>股票 {stock_code} 数据不足30天</h1></body></html>")
                    return
                
                chart_config = self.prepare_klinechart_data(df, stock_code)
                if chart_config:
                    self.show_klinechart(chart_config)
            except Exception as e:
                debug_print("加载或显示K线图时出错:", e)
                import traceback; traceback.print_exc()
                self.browser.setHtml(f"<html><body><h1>操作失败:</h1><pre>{e}</pre></body></html>")

    def prepare_klinechart_data(self, df, stock_code):
        debug_print(f"为 KLineCharts 准备数据: {stock_code}")
        plot_df = df.copy()
        
        kline_data = []
        for _, row in plot_df.iterrows():
            kline_data.append({
                'timestamp': int(row['日期'].timestamp() * 1000),
                'open': float(row['开盘']),
                'high': float(row['最高']),
                'low': float(row['最低']),
                'close': float(row['收盘']),
                'volume': float(row['成交量'])
            })

        stock_name_series = self.stock_pool[self.stock_pool['ts_code'] == stock_code]['name']
        stock_name = stock_name_series.iloc[0] if not stock_name_series.empty else stock_code
        
        # 定义您需要的均线周期
        price_ma_periods = [5, 10, 20, 30]
        
        return {
            'klineData': kline_data,
            'stockName': stock_name,
            'stockCode': stock_code,
            'priceMaPeriods': price_ma_periods
        }

    # +++ 修复后的 show_klinechart 方法 +++
    def show_klinechart(self, config):
        debug_print("显示 KLineChart")
        
        kline_data_js = json.dumps(config['klineData'])
        stock_name_js = json.dumps(config['stockName'])
        stock_code_js = json.dumps(config['stockCode'])
        price_ma_periods_js = json.dumps(config['priceMaPeriods'])
        
        js_path = os.path.join(self.project_root, 'js', 'klinecharts.min.js')
        
        try:
            with open(js_path, 'r', encoding='utf-8') as f:
                klinecharts_js_code = f.read()
        except FileNotFoundError:
            self.browser.setHtml(f"<h1>错误: 未找到 klinecharts.min.js</h1><p>请确保文件存在于: {js_path}</p>")
            return

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>KLineChart</title>
            <style>
                body {{ margin: 0; padding: 0; overflow: hidden; }}
                #kline-chart-container {{
                    width: 100vw;
                    height: 100vh;
                }}
            </style>
        </head>
        <body>
            <div id="kline-chart-container"></div>
            <script>
                {klinecharts_js_code}
            </script>
            <script>
                document.addEventListener('DOMContentLoaded', function() {{
                    const chart = klinecharts.init('kline-chart-container', {{
                        header: {{
                           display: true,
                           title: '{{shortName}}  {{symbol}}'
                        }}
                    }});
                    
                    chart.applyNewData({kline_data_js}, true, {{
                        shortName: {stock_name_js},
                        symbol: {stock_code_js}
                    }});
                    
                    // +++ 最终修复: 一次性创建MA指标，并传入所有周期 +++
                    const priceMaPeriods = {price_ma_periods_js};
                    
                    // 1. 创建一次 MA 指标
                    chart.createIndicator(
                        {{ 
                            name: 'MA', 
                            // 2. 将所有周期 [5, 10, 20, 30] 作为 calcParams 数组传入
                            calcParams: priceMaPeriods 
                        }}, 
                        false, 
                        {{ id: 'candle_pane' }}
                    );
                    
                    // 创建 VOL 指标
                    chart.createIndicator({{ name: 'VOL' }}, false, {{ id: 'volume_pane' }});
                }});
            </script>
        </body>
        </html>
        """
        self.browser.setHtml(html_content, baseUrl=QUrl.fromLocalFile(self.project_root + os.sep))


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