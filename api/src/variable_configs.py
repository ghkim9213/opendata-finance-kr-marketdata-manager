from .config_tools import find_item_by_name, append_model_name

SINGLE_ACCOUNT_CONFIGS = [
    {
        'fs_div': 'BS',
        'name': 'assets',
        'cfs': True
    },
    {
        'fs_div': 'BS',
        'name': 'current_assets',
        'cfs': True
    },
    {
        'fs_div': 'BS',
        'name': 'current_liabilities',
        'cfs': True
    },
    {
        'fs_div': 'BS',
        'name': 'inventories',
        'cfs': True
    },
    {
        'fs_div': 'BS',
        'name': 'liabilities',
        'cfs': True
    },
    {
        'fs_div': 'BS',
        'name': 'equity',
        'cfs': True
    },
    {
        'fs_div': 'BS',
        'name': 'equity_attributable_to_owners_of_parent',
        'cfs': True
    },
    {
        'fs_div': 'BS',
        'name': 'issued_capital',
        'cfs': True
    },
    {
        'fs_div': 'BS',
        'name': 'issued_capital_of_common_stock',
        'cfs': True
    },
    {
        'fs_div': 'PL',
        'name': 'gross_profit',
        'cfs': True
    },
    {
        'fs_div': 'PL',
        'name': 'profit_loss',
        'cfs': True
    },
    {
        'fs_div': 'PL',
        'name': 'revenue',
        'cfs': True
    },
    {
        'fs_div': 'PL',
        'name': 'operating_income_loss',
        'cfs': True
    },
    {
        'fs_div': 'PL',
        'name': 'profit_loss_attributable_to_owners_of_parent',
        'cfs': True
    },
    {
        'fs_div': 'PL',
        'name': 'total_selling_general_administrative_expenses',
        'cfs': True,
    },
]
SINGLE_ACCOUNT_CONFIGS = append_model_name('SingleAccount', SINGLE_ACCOUNT_CONFIGS)


MIXED_ACCOUNT_CONFIGS = [
    {
        'name': 'book_equity',
        'ordered_account_confs': [
            find_item_by_name('issued_capital', SINGLE_ACCOUNT_CONFIGS),
            find_item_by_name('issued_capital_of_common_stock', SINGLE_ACCOUNT_CONFIGS),
        ],
    },
]
MIXED_ACCOUNT_CONFIGS = append_model_name('MixedAccount', MIXED_ACCOUNT_CONFIGS)


ACCOUNT_RATIO_CONFIGS = [
    {
        'name': 'gross_profitability',
        'numerator': find_item_by_name('gross_profit', SINGLE_ACCOUNT_CONFIGS),
        'denominator': find_item_by_name('assets', SINGLE_ACCOUNT_CONFIGS),
    },
    {
        'name': 'operating_profitability',
        'numerator': find_item_by_name('operating_income_loss', SINGLE_ACCOUNT_CONFIGS),
        'denominator': find_item_by_name('book_equity', MIXED_ACCOUNT_CONFIGS),
    },
    {
        'name': 'current_ratio',
        'numerator': find_item_by_name('current_assets', SINGLE_ACCOUNT_CONFIGS),
        'denominator': find_item_by_name('current_liabilities', SINGLE_ACCOUNT_CONFIGS),
    },
    {
        'name': 'debt_to_equity',
        'numerator': find_item_by_name('liabilities', SINGLE_ACCOUNT_CONFIGS),
        'denominator': find_item_by_name('equity', SINGLE_ACCOUNT_CONFIGS),
    },
    {
        'name': 'equity_to_assets',
        'numerator': find_item_by_name('equity', SINGLE_ACCOUNT_CONFIGS),
        'denominator': find_item_by_name('assets', SINGLE_ACCOUNT_CONFIGS),
    },
    {
        'name': 'gross_profit_margin',
        'numerator': find_item_by_name('gross_profit', SINGLE_ACCOUNT_CONFIGS),
        'denominator': find_item_by_name('revenue', SINGLE_ACCOUNT_CONFIGS),
    },
    {
        'name': 'operating_profit_margin',
        'numerator': find_item_by_name('operating_income_loss', SINGLE_ACCOUNT_CONFIGS),
        'denominator': find_item_by_name('revenue', SINGLE_ACCOUNT_CONFIGS),
    },
    {
        'name': 'return_on_assets',
        'numerator': find_item_by_name('profit_loss', SINGLE_ACCOUNT_CONFIGS),
        'denominator': find_item_by_name('assets', SINGLE_ACCOUNT_CONFIGS),
    },
    {
        'name': 'return_on_equity',
        'numerator': find_item_by_name('profit_loss_attributable_to_owners_of_parent', SINGLE_ACCOUNT_CONFIGS),
        'denominator': find_item_by_name('equity_attributable_to_owners_of_parent', SINGLE_ACCOUNT_CONFIGS),
    },
    {
        'name': 'asset_turnover',
        'numerator': find_item_by_name('revenue', SINGLE_ACCOUNT_CONFIGS),
        'denominator': find_item_by_name('assets', SINGLE_ACCOUNT_CONFIGS),
    },
]
ACCOUNT_RATIO_CONFIGS = append_model_name('AccountRatio', ACCOUNT_RATIO_CONFIGS)


PRICE_RATIO_CONFIGS = [
    {
        'name': 'book_to_market',
        'numerator': find_item_by_name('book_equity', MIXED_ACCOUNT_CONFIGS),
    },
    {
        'name': 'earnings_to_price',
        'numerator': find_item_by_name('profit_loss_attributable_to_owners_of_parent', SINGLE_ACCOUNT_CONFIGS),
    },
]
PRICE_RATIO_CONFIGS = append_model_name('PriceRatio', PRICE_RATIO_CONFIGS)


MOMENTUM_CONFIGS = [
    {'near': 2, 'far': 12},
]
MOMENTUM_CONFIGS = append_model_name('Momentum', MOMENTUM_CONFIGS)


ETC = [
    {'model_name': 'Size', 'name': 'market_equity', 'label_en': 'market equity', 'label_kr': '시가총액'},
]
# ETC = {
#     'size': None #MarketEquity(),
# }

VARIABLE_CONFIGS = {
    'single_account': SINGLE_ACCOUNT_CONFIGS,
    'mixed_account': MIXED_ACCOUNT_CONFIGS,
    'account_ratio': ACCOUNT_RATIO_CONFIGS,
    'price_ratio': PRICE_RATIO_CONFIGS,
    'momentum': MOMENTUM_CONFIGS,
    'etc': ETC,
    # 'etc': {
    #     'list': etc,
    #     'freq':
    # }
}
