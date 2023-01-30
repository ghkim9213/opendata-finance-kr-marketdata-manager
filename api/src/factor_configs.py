# data defining factor portfolio managers
from .config_tools import find_item_by_name
from .variable_configs import VARIABLE_CONFIGS

DEFAULT_QUANTILES = [0, .3, .7, 1]
DEFAULT_LABELS = ['bottom', 'middle', 'top']
DEFAULT_LOOKBACK = 6

UNIVARIATE_FACTOR_CONFIGS = [
    {
        'variable': find_item_by_name('book_to_market', VARIABLE_CONFIGS['price_ratio']),
        'quantiles': DEFAULT_QUANTILES,
        'labels': DEFAULT_LABELS,
        'lookback': DEFAULT_LOOKBACK,
    },
    {
        'variable': find_item_by_name('earnings_to_price', VARIABLE_CONFIGS['price_ratio']),
        'quantiles': DEFAULT_QUANTILES,
        'labels': DEFAULT_LABELS,
        'lookback': DEFAULT_LOOKBACK,
    },
    {
        'variable': find_item_by_name('operating_profitability', VARIABLE_CONFIGS['account_ratio']),
        'quantiles': DEFAULT_QUANTILES,
        'labels': DEFAULT_LABELS,
        'lookback': DEFAULT_LOOKBACK,
    },
    {
        'variable': find_item_by_name('market_equity', VARIABLE_CONFIGS['etc']),
        'quantiles': DEFAULT_QUANTILES,
        'labels': DEFAULT_LABELS,
        'lookback': DEFAULT_LOOKBACK,
    },
] + [
    {
        'variable': mom_conf,
        'quantiles': DEFAULT_QUANTILES,
        'labels': DEFAULT_LABELS,
        'lookback': DEFAULT_LOOKBACK,
    } for mom_conf in VARIABLE_CONFIGS['momentum']
]

SIZE_VALUE_FACTOR = [
    {
        'variable': find_item_by_name('market_equity', VARIABLE_CONFIGS['etc']),
        'quantiles': [0, .5, 1],
        'labels': ['small', 'big'],
        'lookback': DEFAULT_LOOKBACK,
    },
    {
        'variable': find_item_by_name('book_to_market', VARIABLE_CONFIGS['price_ratio']),
        'quantiles': DEFAULT_QUANTILES,
        'labels': ['growth', 'neutral', 'value'],
        'lookback': DEFAULT_LOOKBACK,
    },
]

MULTIVARIATE_FACTOR_CONFIGS = [
    SIZE_VALUE_FACTOR,
]
