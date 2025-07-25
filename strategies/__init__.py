from .high_price_filter import is_selected as high_price_filter
from .ma_crossover import is_selected as ma_crossover
from .n_limit_up import is_selected as n_limit_up

STRATEGIES = {
    "高价股筛选": high_price_filter,
    "均线金叉": ma_crossover,
    "N连板": n_limit_up
}