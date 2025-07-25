# strategies/__init__.py

from .n_limit_up import is_selected as n_limit_up_strategy
from .ma_crossover import is_selected as ma_crossover_strategy
from .high_price_filter import is_selected as high_price_filter_strategy 
STRATEGIES = {
    "n_limit_up": n_limit_up_strategy,
    "ma_crossover": ma_crossover_strategy,
    "high_price_filter": high_price_filter_strategy,
}