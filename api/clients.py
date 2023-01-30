from datetime import timedelta
from dateutil.relativedelta import relativedelta
from django.conf import settings
from json.decoder import JSONDecodeError
from time import sleep

import datetime
import requests
import xml.etree.ElementTree as XmlParser
import json

# Api clients for data.go.kr
class OpenApiClient:
    MIN_DATE = settings.OPENAPI_DATA_STARTS_ON

    def __init__(self, name, endpoint):
        self.name = name
        self.endpoint = endpoint
        self.service_key_type = self.DEFAULT_SERVICE_KEY_TYPE
        self.result_type = 'json'

    def get_service_key(self):
        if self.service_key_type == 'ENC':
            return settings.OPENAPI_SERVICE_KEY_ENCODED
        else:
            return settings.OPENAPI_SERVICE_KEY_DECODED

    @property
    def DEFAULT_SERVICE_KEY_TYPE(self):
        return 'DEC'

    @property
    def base_parameters(self):
        return {
            'serviceKey': self.get_service_key(),
            'resultType': self.result_type,
        }

    def switch_service_key_type(self):
        alternative = 'ENC' if self.service_key_type == 'DEC' else 'DEC'
        self.service_key_type = alternative
        return alternative

    def query(self, params):
        res = self.requests(params, total=True)
        try:
            d = self.parse_response(res)
        except OpenApiResponsesXmlError:
            print('Fail to querying data from open api. Waiting for 5 minutes to try again...')
            sleep(300)
            self.query(params)
        sleep(10)
        return self.get_records(d)

    def get_records(self, d):
        return d.get('response').get('body').get('items').get('item')

    def requests(self, params, total=True):
        if total:
            _params = self.append_total_count(params)
            return requests.get(self.endpoint, {**self.base_parameters, **_params})
        return requests.get(self.endpoint, {**self.base_parameters, **params})

    def parse_response(self, response):
        try:
            return response.json()
        except JSONDecodeError:
            xml_data = XmlParser.fromstring(response.text)
            msg = XmlParser.dump(xml_data)
            raise OpenApiResponsesXmlError(msg)

    def append_total_count(self, params):
        _params = {**self.base_parameters, **params, 'pageNo': 1, 'numOfRows': 1}
        res = self.requests(_params, total=False)
        d = self.parse_response(res)
        total_count = d.get('response').get('body').get('totalCount')
        return {**params, 'pageNo': 1, 'numOfRows': total_count}

    def verify_latest(self):
        today = datetime.date.today()
        dt = today
        while True:
            verifying_params = {
                **self.base_parameters,
                'pageNo': 1,
                'numOfRows': 1,
                'basDt': dt.strftime('%Y%m%d')
            }
            res = self.requests(verifying_params, total=False)
            d = self.parse_response(res)
            total_count = d.get('response').get('body').get('totalCount')
            if total_count > 0:
                break
            dt -= timedelta(days=1)
        return dt

    def verify_trading_monthend(self, real_monthend):
        dt = real_monthend
        while True:
            verifying_params = {
                **self.base_parameters,
                'pageNo': 1,
                'numOfRows': 1,
                'basDt': dt.strftime('%Y%m%d')
            }
            res = self.requests(verifying_params, total=False)
            d = self.parse_response(res)
            total_count = d.get('response').get('body').get('totalCount')
            if total_count > 0:
                break
            dt -= timedelta(days=1)
        return dt


class CorpListApiClient(OpenApiClient):
    def __init__(self):
        super().__init__(
            name = 'corp_list',
            endpoint = 'http://apis.data.go.kr/1160100/service/GetKrxListedInfoService/getItemInfo'
        )

class StockPriceApiClient(OpenApiClient):
    def __init__(self):
        super().__init__(
            name = 'stock_prices',
            endpoint = 'http://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo'
        )

class OpenApiResponsesXmlError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return f'''
            OpenApi responses the following as xml while requested json:
            {self.msg}
        '''
