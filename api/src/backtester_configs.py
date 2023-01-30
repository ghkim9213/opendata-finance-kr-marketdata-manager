from .factor_configs import UNIVARIATE_FACTOR_CONFIGS
from .factor_configs import MULTIVARIATE_FACTOR_CONFIGS

UNIVARIATE_BACKTESTER_CONFIGS = [
    {**r, 'rebalancing_frequency': 12}
    for r in UNIVARIATE_FACTOR_CONFIGS
] + [] # append additional configs with different rb freq

MULTIVARIATE_BACKTESTER_CONFIGS = [
    {
        'factors': ls,
        'rebalancing_frequency': 12,
    } for ls in MULTIVARIATE_FACTOR_CONFIGS
] + []
