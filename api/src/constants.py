import datetime

OPENDART_TEXTFILE_HEADER_INFO = {
    '재무제표종류': {'label_en': 'rpt_div', 'type': str},
    '종목코드': {'label_en': 'stock_code', 'type': str},
    '회사명': {'label_en': 'stock_name', 'type': str},
    '시장구분': {'label_en': 'market', 'type': str},
    '업종': {'label_en': 'industry_code', 'type': str},
    '업종명': {'label_en': 'industry_name', 'type': str},
    '결산월': {'label_en': 'fye', 'type': int},
    '결산기준일': {'label_en': 'date', 'type': datetime.date},
    '보고서종류': {'label_en': 'rpt_type', 'type': str},
    '통화': {'label_en': 'currency', 'type': str},
    '항목코드': {'label_en': 'acnt_id', 'type': str},
    '항목명': {'label_en': 'label_kr', 'type': str},
}

OPENDART_TEXTFILE_VALUE_HEADER_MAP = {
    '1Q_BS': '당기 1분기말', # bs / q1
    '1Q_PL': '당기 1분기 3개월', # is, cis /  q1
    '1Q_CF': '당기 1분기', # cf / q1

    '2Q_BS': '당기 반기말', # bs / q2
    '2Q_PL': '당기 반기 3개월', # is, cis / q2
    '2Q_CF': '당기 반기', # cf / q2

    '3Q_BS': '당기 3분기말', # bs / q3
    '3Q_PL': '당기 3분기 3개월', # is, cis / q3
    '3Q_CF': '당기 3분기', # cf / q3

    '4Q': '당기', # all / q4
}

SINGLE_ACCOUNT_CLIENT_RPTDIV_TO_SJDIV_MAP = {
    '재무상태표, 유동/비유동법-연결재무제표': 'BS1',
    '재무상태표, 유동/비유동법-별도재무제표': 'BS2',
    '재무상태표, 유동성배열법-연결재무제표': 'BS3',
    '재무상태표, 유동성배열법-별도재무제표': 'BS4',

    '손익계산서, 기능별 분류 - 연결재무제표': 'IS1',
    '손익계산서, 기능별 분류 - 별도재무제표': 'IS2',
    '손익계산서, 성격별 분류 - 연결재무제표': 'IS3',
    '손익계산서, 성격별 분류 - 별도재무제표': 'IS4',

    '포괄손익계산서(세후기타포괄손익) - 연결재무제표': 'CIS1',
    '포괄손익계산서(세후기타포괄손익) - 별도재무제표': 'CIS2',
    '포괄손익계산서(세전기타포괄손익) - 연결재무제표(선택)': 'CIS3',
    '포괄손익계산서(세전기타포괄손익) - 별도재무제표(선택)': 'CIS4',

    '포괄손익계산서, 기능별 분류(세후기타포괄손익) - 연결재무제표': 'DCIS1',
    '포괄손익계산서, 기능별 분류(세후기타포괄손익) - 별도재무제표': 'DCIS2',
    '포괄손익계산서, 기능별 분류(세전기타포괄손익) - 연결재무제표(선택)': 'DCIS3',
    '포괄손익계산서, 기능별 분류(세전기타포괄손익) - 별도재무제표(선택)': 'DCIS4',

    '포괄손익계산서, 성격별 분류(세후기타포괄손익) - 연결재무제표': 'DCIS5',
    '포괄손익계산서, 성격별 분류(세후기타포괄손익) - 별도재무제표': 'DCIS6',
    '포괄손익계산서, 성격별 분류(세전기타포괄손익) - 연결재무제표(선택)': 'DCIS7',
    '포괄손익계산서, 성격별 분류(세전기타포괄손익) - 별도재무제표(선택)': 'DCIS8',

    '현금흐름표, 직접법 - 연결재무제표': 'CF1',
    '현금흐름표, 직접법 - 별도재무제표': 'CF2',
    '현금흐름표, 간접법 - 연결재무제표': 'CF3',
    '현금흐름표, 간접법 - 별도재무제표': 'CF4',
}

SINGLE_ACCOUNT_CLIENT_MARKET_LABEL_KR_TO_EN_MAP = {
    '유가증권시장상장법인': 'KOSPI',
    '코스닥시장상장법인': 'KOSDAQ',
}


CORP_LIST_DATA_RENAME_MAP = {
    'basDt': 'date',
    'mrktCtg': 'market',
    'srtnCd': 'stock_code',
    'isinCd': 'isin_code',
    'itmsNm': 'name',
    'crno': 'crno',
    'corpNm': 'corp_name',
}

STOCK_PRICE_DATA_RENAME_MAP = {
    'basDt': 'date',
    'mrktCtg': 'market',
    'srtnCd': 'stock_code',
    'isinCd': 'isin_code',
    'itmsNm': 'name',
    'clpr': 'close',
    'fltRt': 'ri',
    'mkp': 'open',
    'hipr': 'high',
    'lopr': 'low',
    'trqu': 'vol_n',
    'trPrc': 'vol_m',
    'lstgStCnt': 'n_listed',
    'mrktTotAmt': 'mktcap',
}
