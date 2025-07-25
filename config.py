# config.py

SELECTED_STRATEGY = "n_limit_up"  # 当前使用的选股策略模块名
# SELECTED_STRATEGY = "ma_crossover"  # 改成你要使用的策略名
# SELECTED_STRATEGY = "high_price_filter" 
MAX_RETRIES = 3                   # 接口最大重试次数
MIN_INTERVAL = 2                  # 初始请求间隔（秒）
MAX_INTERVAL = 60                 # 最大请求间隔（秒）

MASTER_DATA_FILE = 'master_stock_data.feather'
SNAPSHOT_FILE = 'snapshot_data.feather'
STOCK_POOL_FILE = 'stock_pool.csv'

N_CONSECUTIVE_DAYS = 1           # 默认连板天数
DEBUG_STOCK_CODE = None           # 设置调试股票代码（如 '000514.SZ'）