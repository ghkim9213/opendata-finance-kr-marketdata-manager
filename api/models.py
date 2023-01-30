from .managers import (
    OpendartZipfileManager,
    StockPriceManager,
    CorpListManager,
    SingleAccountClientManager,
    SingleAccountManager,
    MixedAccountManager,
    AccountRatioManager,
    PriceRatioManager,
    MomentumManager,
    BacktesterManager,
)
from .tools import (
    convert_records_to_csv,
    create_zipfile,
)
from .src import constants

from datetime import timedelta
from dateutil.relativedelta import relativedelta
from django.db import models
from django.db.models import Max
from django.conf import settings
from functools import reduce
from io import BytesIO
from itertools import product, zip_longest

import datetime
import json
import requests
import numpy as np
import pandas as pd
import zipfile

def DEFAULT_DICT():
    return {}

def DEFAULT_LIST():
    return []


class OpendartFile(models.Model):
    identifier = models.CharField(max_length=128)

    class Meta:
        abstract = True


class OpendartZipfile(OpendartFile):
    last_update = models.DateTimeField(default=datetime.datetime.min)
    file = models.FileField(upload_to='opendart-financial-statements-clone', null=True)
    objects = OpendartZipfileManager()

    class Meta:
        db_table = 'source_opendart_zipfile'

    def __str__(self):
        return f"{self.identifier}_{self.last_update.strftime('%Y%m%d%H%M%S')}.zip"

    def download_from_source(self):
        url = 'https://opendart.fss.or.kr/cmm/downloadFnlttZip.do'
        payload = {'fl_nm': self.__str__()}
        headers = {
            'Referer':'https://opendart.fss.or.kr/disclosureinfo/fnltt/dwld/main.do',
            'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36',
        }
        r = requests.get(url, payload, headers=headers)
        return BytesIO(r.content)

    def get_file(self):
        self.file.seek(0)
        return zipfile.ZipFile(BytesIO(self.file.read()))

    def get_clean_text_filename(self, text_filename):
        return text_filename.encode('cp437').decode('cp949')

    def bootstrap_text_files(self):
        zf = self.get_file()
        for tfnm in zf.namelist():
            clean_tfnm = self.get_clean_text_filename(tfnm)
            d = self.parse_text_filename(clean_tfnm)
            odtf, created = OpendartTextfile.objects.get_or_create(
                is_in = self,
                identifier = d['identifier']
            )
            updated = odtf.last_update != d['last_update']
            if updated:
                odtf.last_update = d['last_update']
                odtf.save()
            if created or updated:
                tf = zf.open(tfnm)
                odtf.write_contains(use_file=tf)

    def parse_text_filename(self, clean_text_filename):
        parsed = clean_text_filename.split('.')[0].split('_')
        return {
            'identifier': '_'.join(parsed[:-1]),
            'last_update': datetime.datetime.strptime(parsed[-1], '%Y%m%d').date()
        }


class OpendartTextfile(OpendartFile):
    is_in = models.ForeignKey(
        OpendartZipfile,
        related_name = 'text_files',
        on_delete = models.CASCADE
    )
    last_update = models.DateField(default=datetime.date.today)
    contains = models.JSONField(default=DEFAULT_LIST)

    class Meta:
        db_table = 'source_opendart_textfile'

    def __str__(self):
        return f"{self.identifier}_{self.last_update.strftime('%Y%m%d')}.txt"

    def write_contains(self, **kwargs):
        f = kwargs.get('use_file')
        df = pd.DataFrame.from_records(self.get_records(use_file=f))
        is_std = (~df.acnt_id.str.startswith('entity'))
        df['acnt_nm'] = df.acnt_id.str.split('_').str[1]
        self.contains = df[is_std][['rpt_div', 'acnt_nm']].drop_duplicates().to_dict(orient='records')
        self.save()
        return self.contains

    def get_records(self, **kwargs):
        f = kwargs.get('use_file')
        if not f:
            dirty_fnm = self.get_dirty_filename()
            zf = self.is_in.get_file()
            f = zf.open(dirty_fnm)
        f.seek(0)
        records = []
        parse_row = lambda row: row.decode('cp949').replace('\r\n', '').split('\t')
        h = next(f)
        h_p = parse_row(h)
        for r in f:
            r_p = parse_row(r)
            records.append(dict(zip_longest(h_p, r_p)))
        f.close()
        # drop unnecessaries and rename
        return [{
            v['label_en']: r[k] for k, v in self.HEADERS.items()
        } for r in records]

    def get_dataframe(self, **kwargs):
        f = kwargs.get('use_file')
        if f:
            df = pd.DataFrame.from_records(self.get_records(use_file=f))
        else:
            df = pd.DataFrame.from_records(self.get_records())
        df = df[self.HEADERS.keys()] # keep necessary fields only
        rename_map = {k: v['label_en'] for k, v in self.HEADERS.items()}
        df = df.rename(columns=rename_map)
        for props in self.HEADERS.values():
            c = props['label_en']
            tp = props['type']
            if tp == datetime.date:
                df[c] = pd.to_datetime(df[c], format='%Y-%m-%d')
            else:
                if c == 'value':
                    df[c] = df[c].str.replace(',', '').replace('', None).astype(tp)
                elif c == 'stock_code':
                    df[c] = df[c].str[1:-1]
                else:
                    df[c] = df[c].astype(tp)
        return df


    def get_dirty_filename(self):
        return self.__str__().encode('cp949').decode('cp437')

    @property
    def HEADERS(self):
        return {
            **self.INDEX_HEADERS,
            **self.value_header,
        }

    @property
    def INDEX_HEADERS(self):
        return constants.OPENDART_TEXTFILE_HEADER_INFO

    @property
    def value_header(self):
        return {
            self.VALUE_HEADER_MAP[self.value_header_identifier]: {
                'label_en': 'value',
                'type': float # int but use float to manipulate missing values
            }
        }

    @property
    def VALUE_HEADER_MAP(self):
        return constants.OPENDART_TEXTFILE_VALUE_HEADER_MAP

    @property
    def value_header_identifier(self):
        if '4Q' in self.is_in.identifier:
            return '4Q'
        else:
            return '_'.join(self.is_in.identifier.split('_')[1:])


# While SingleAccountClient is a django model, it is classifed as client
# because of its fuctionality.
# This is a client for OPENDART data.

class SingleAccountClient(models.Model):
    FS_DIV_CHOICES = [
        ('BS', 'balance sheet'),
        ('PL', 'income statement'),
        ('CF', 'cashflow statement'),
    ]
    name = models.CharField(max_length=128)
    label_en = models.CharField(max_length=128, null=True, blank=True)
    label_kr = models.CharField(max_length=128, null=True, blank=True)
    fs_div = models.CharField(max_length=2, choices=FS_DIV_CHOICES)
    cfs = models.BooleanField(default=True)
    file = models.FileField(upload_to='opendart-account-panel', null=True)
    last_update = models.DateField(default=datetime.date.min)
    objects = SingleAccountClientManager()

    class Meta:
        db_table = 'source_single_account_client'

    def __str__(self):
        return f"{self.fs_div}_{self.capitalized_name}_{self.oc_label}"

    @property
    def oc_label(self):
        return 'CFS' if self.cfs else 'OFS'

    @property
    def capitalized_name(self):
        return ''.join([x.capitalize() for x in self.name.split('_')])


    def sync_to_sources(self):
        last_update = self.get_last_update_of_sources()
        if self.last_update >= last_update:
            print(f"{self} have already been synced to sources.")
            return
        self.write_file()
        self.last_update = last_update
        self.save()

    def get_file(self):
        self.file.seek(0)
        return zipfile.ZipFile(BytesIO(self.file.read()))

    def get_records_from_sources(self):
        d_sources = self.list_sources(to_dict=True)
        std_records = []
        nstd_records = []
        for zf_id, ls_tf_id in d_sources.items():
            zf = OpendartZipfile.objects.get(id=zf_id)
            _zf = zf.get_file()
            for tf_id in ls_tf_id:
                tf = OpendartTextfile.objects.get(id=tf_id)
                tfnm = tf.get_dirty_filename()
                _tf = _zf.open(tfnm)
                records = tf.get_records(use_file=_tf)
                for r in records:
                    acnt_id = r.get('acnt_id')
                    if acnt_id.startswith('entity'):
                        nstd_records.append(r)
                        continue
                    acnt_nm = acnt_id.split('_')[-1]
                    if acnt_nm != self.capitalized_name:
                        continue
                    std_records.append(r)

        set_std_label_kr = set(list(map(
            lambda r: r['label_kr'].replace(' ',''),
            std_records
        )))

        nstd_records = list(filter(
            lambda r: r['label_kr'].replace(' ','') in set_std_label_kr,
            nstd_records
        ))

        return std_records + nstd_records

    @property
    def CSV_FILENAME(self):
        return f"{self.__str__()}.csv"

    @property
    def ZIP_FILENAME(self):
        return f"{self.__str__()}.zip"

    def write_file(self, return_file=False, **kwargs):
        use_records = kwargs.get('use_records')
        if use_records:
            records = use_records
        else:
            records = self.get_records_from_sources()

        files_to_zip = [{
            'name': self.CSV_FILENAME,
            'file': convert_records_to_csv(records)
        }]
        zf = create_zipfile(files_to_zip)
        self.file.save(self.ZIP_FILENAME, zf)
        print(f"{self.__str__()}.zip was saved on cloud storage.")
        if return_file:
            return self.get_file()

    def list_sources(self, file_name=False, to_dict=False):
        qs_zf = OpendartZipfile.objects.filter(identifier__contains=self.fs_div)
        ls_sources = []
        for zf in qs_zf:
            if self.cfs:
                qs_tf = zf.text_files.filter(identifier__contains='연결')
            else:
                qs_tf = zf.text_files.exclude(identifier__contains='연결')

            for tf in qs_tf:
                if any([x['acnt_nm'] == self.capitalized_name for x in tf.contains]):
                    if file_name:
                        s = (zf.__str__(), tf.__str__())
                    else:
                        s = (zf.id, tf.id)
                    ls_sources.append(s)
        if not to_dict:
            return ls_sources
        d = {}
        for z, t in ls_sources:
            if z not in d.keys():
                d[z] = []
            if t not in d[z]:
                d[z].append(t)
        return d

    def get_last_update_of_sources(self):
        ls_sources = self.list_sources(file_name=True, to_dict=False)
        dt_max = None
        for zfnm, tfnm in ls_sources:
            dt = datetime.datetime.strptime(
                tfnm.split('.')[0].split('_')[-1],
                '%Y%m%d'
            ).date()
            if not dt_max:
                dt_max = dt
            if dt_max < dt:
                dt_max = dt
        return dt_max

    def get_dataframe(self):
        zf = self.get_file()
        df = pd.read_csv(zf.open(self.CSV_FILENAME))
        df = self._clean_dataframe(df)
        df = self._keep_dominant_sj_div(df)
        if self.fs_div == 'BS':
            return df
        df = self._append_value_q(df)
        df = self._append_value_y(df)
        return df

    def _clean_dataframe(self, df):
        df['sj_div'] = df.rpt_div.replace(self.RPTDIV_TO_SJDIV_MAP)
        df = df.loc[df.currency=='KRW'].copy()
        df.stock_code = df.stock_code.str[1:-1]
        df.market = df.market.replace(self.MARKET_LABEL_KR_TO_EN_MAP)
        df.date = pd.to_datetime(df.date)
        df.fye = df.fye.astype(int)
        df.value = df.value.str.replace(',','').astype(float)
        result = df.copy().dropna()
        result.value = result.value.astype(int)
        return result

    def _keep_dominant_sj_div(self, df):
        result = df.copy()

        # get priority by count
        indexes = [ c for c in df.columns if c != 'value']
        dfw = df.set_index(indexes).unstack('sj_div')
        sj_div_ord_by_count = (~dfw.value.isnull()).sum().sort_values(ascending=False).index
        sj_div_priority = {v: i for i, v in enumerate(sj_div_ord_by_count)}

        # keep dominant only
        result['priority'] = result.sj_div.replace(sj_div_priority)
        result = result.sort_values(['stock_code', 'date', 'priority'])
        dups = result.duplicated(['stock_code', 'date'])
        # print(f"{dups.sum()} dominated fs value were dropped.")
        del result['priority']
        return result[~dups]

    def _append_value_q(self, df):
        # caculate by and bq from date and fye
        mgap = df.date.dt.month - df.fye
        is_after_fye = (mgap <=0) & (df.fye !=12)
        ydiff = -(is_after_fye.astype(int))
        df['by'] = df.date.dt.year + ydiff
        df['bq'] = mgap.replace({-9:3, -6:6, -3:9, 0:12}) // 3

        # Drop ambiguous rpt_type.
        # Most of ambiguous rpt_type are originated from change in fye.
        bq_rpt_type = df.bq.replace({
            1: '1분기보고서',
            2: '반기보고서',
            3: '3분기보고서',
            4: '사업보고서',
        })
        is_clear_rpt_type = df.rpt_type == bq_rpt_type
        df = df[is_clear_rpt_type]

        # convert annual to quarter
        dfv = df[['stock_code', 'by', 'bq', 'value']]
        # print(f"{(~is_clear_rpt_type).sum()} ambiguous values were dropped.")

        # Drop duplicates
        # All duplicates in ['stock_code', 'by', 'bq'] are originated from change in fye.
        # Right after change, a fs is reported with different values.
        # We take the right-before value to keep data consistency.
        # dfi should be sorted by ['stock_code', 'date']
        dups = dfv.duplicated(['stock_code', 'by', 'bq'])
        dfv = dfv.copy()[~dups]
        # print(f"{dups.sum()} duplicated values were dropped.")

        dfq = dfv.set_index(['stock_code', 'by', 'bq']).unstack('bq').value
        dfq[4] = dfq[4] - (dfq[1] + dfq[2] + dfq[3])
        new_val = dfq.stack().rename('value_q').reset_index()
        dfvv = dfv.reset_index().merge(new_val, on=['stock_code', 'by', 'bq'], how='left')
        df = df.reset_index().merge(dfvv[['index', 'value_q']], on=['index'], how='left')
        del df['index'], df['by'], df['bq']
        return df

    def _append_value_y(self, df):
        tdiff = df.date - df.groupby('stock_code').date.shift(1)
        is_not_big_gap = (tdiff <= '92 days') | (tdiff.isnull())
        s_omitted_on_big_gap = (is_not_big_gap * df.value_q).replace(0, np.nan)
        df['value_y'] = s_omitted_on_big_gap.groupby(df.stock_code).rolling(4).sum().values
        return df

    def get_records(self):
        df = self.get_dataframe()
        return df.to_dict(orient='records')

    def query(self, **kwargs):
        params = kwargs.get('params')
        filtered = self.get_dataframe()[self.RESPONSE_PARAMETERS]
        if not params:
            pass
        elif any([k not in self.REQUEST_PARAMETERS for k in params.keys()]):
            raise Exception('Invalid query parameter.')
        else:
            for k, v in params.items():
                if k == 'date':
                    v = pd.to_datetime(v)
                filtered = filtered.loc[filtered[k] == v]
        filtered.date = filtered.date.astype(str).str.replace('-','')
        return json.loads(filtered.to_json(orient='records'))

    @property
    def REQUEST_PARAMETERS(self):
        return ['date', 'stock_code']

    @property
    def RESPONSE_PARAMETERS(self):
        base = [
            'market', 'stock_code', 'date',
            'rpt_type', 'acnt_id', 'value',
        ]
        if self.fs_div == 'BS':
            return base
        else:
            return base + ['value_q', 'value_y']

    @property
    def RPTDIV_TO_SJDIV_MAP(self):
        return constants.SINGLE_ACCOUNT_CLIENT_RPTDIV_TO_SJDIV_MAP

    @property
    def MARKET_LABEL_KR_TO_EN_MAP(self):
        return constants.SINGLE_ACCOUNT_CLIENT_MARKET_LABEL_KR_TO_EN_MAP


class OpenApiData(models.Model):
    date = models.DateField()
    records = models.JSONField(default=DEFAULT_LIST)

    class Meta:
        abstract = True


    def __str__(self):
        return self.date.strftime('%Y-%m-%d')

    @property
    def date_appended_records(self):
        return [{'date': self.date, **r} for r in self.records]

    def get_clean_records(self):
        cleaner = self.record_cleaner
        return [{
            v['rename']: v['typing_method'](r[k])
            for k, v in cleaner.items()
        } for r in self.records]

    @property
    def record_cleaner(self):
        return # override


class CorpList(OpenApiData):
    objects = CorpListManager()

    class Meta:
        db_table = 'corp_list'

    @property
    def RENAME_MAP(self):
        return constants.CORP_LIST_DATA_RENAME_MAP

    @property
    def record_cleaner(self):
        return {
            nm: {
                'rename': rnm,
                'typing_method': str if rnm != 'date' else (lambda dt: datetime.datetime.strptime(dt, '%Y%m%d').date()),
            } for nm, rnm in self.RENAME_MAP.items()
        }

    def write_records(self):
        self.records = CorpListApiClient().query(params={'basDt': self.date.strftime('%Y%m%d')})
        self.save()


class StockPrice(OpenApiData):
    is_monthend = models.BooleanField(default=False)
    objects = StockPriceManager()

    class Meta:
        db_table = 'stock_price'

    @property
    def INDEX_COLUMNS(self):
        return ['date', 'market', 'stock_code']

    def select_columns(self, columns, include_preferred_stocks=False):
        clean_records = self.get_clean_records()
        selected_records = [{
            **{c: r[c] for c in self.INDEX_COLUMNS},
            **{c: r[c] for c in columns}
        } for r in clean_records]
        if include_preferred_stocks:
            return selected_records
        return self.keep_common_stocks_only(selected_records)

    def get_matched_corp_list(self):
        matched_cl, created = CorpList.objects.get_or_create(date=self.date)
        if len(matched_cl.records) == 0:
            matched_cl.write_records()
        records_cl = matched_cl.get_clean_records()
        index_columns_for_cl = ['date', 'market', 'stock_code']
        records_cl = [{
            nm: r[nm] if nm != 'stock_code' else r[nm][1:]
            for nm in index_columns_for_cl
        } for r in records_cl]
        return records_cl

    def keep_common_stocks_only(self, cleaned_records):
        df = pd.DataFrame.from_records(cleaned_records)
        df_cl = pd.DataFrame.from_records(self.get_matched_corp_list())
        return df.merge(df_cl, on=['date', 'stock_code', 'market']).to_dict(orient='records')

    @property
    def RENAME_MAP(self):
        return constants.STOCK_PRICE_DATA_RENAME_MAP

    @property
    def record_cleaner(self):
        intvars = [
            'close', 'open', 'high',
            'low', 'vol_n', 'vol_m',
            'n_listed', 'mktcap',
        ]
        return {
            nm: {
                'rename': rnm,
                'typing_method': (
                    (lambda dt: datetime.datetime.strptime(dt, '%Y%m%d').date())
                    if rnm == 'date' else (
                        (int) if rnm in intvars else (
                            (float) if rnm == 'ri' else (str)
                        )
                    )
                )
            } for nm, rnm in self.RENAME_MAP.items()
        }


class Variable(models.Model):
    name = models.CharField(max_length=128)
    label_en = models.CharField(max_length=128, null=True, blank=True)
    label_kr = models.CharField(max_length=128, null=True, blank=True)
    file = models.FileField(upload_to='products/variables')
    url = models.TextField(null=True)
    last_update = models.DateField(auto_now=True)

    class Meta:
        abstract = True

    def capitalize_name(self):
        return ''.join([x.capitalize() for x in self.name.split('_')])

    def import_variable_data(self, variable_config, to_dataframe=True):
        model = eval(variable_config['model_name'])
        obj = model.objects.get(id=variable_config['id'])
        if not to_dataframe:
            return obj.get_data()
        return pd.DataFrame.from_records(obj.get_data())

    def append_mktcap_column(self, df):
        df['ym'] = df.date.str[:-2]
        s_strdt = df.date.unique()
        prc_data = list()
        for strdt in s_strdt:
            dt = datetime.datetime.strptime(strdt, '%Y%m%d')
            matched = StockPrice.objects.filter(
                date__year = dt.year,
                date__month = dt.month,
                is_monthend = True
            )
            if matched.exists():
                m = matched.first()
                prc_data += m.select_columns(columns=['mktcap'])
        df_prc = pd.DataFrame.from_records(prc_data)
        df_prc['ym'] = df_prc.date.apply(lambda s: s.strftime('%Y%m'))
        del df_prc['date']
        df = df.merge(df_prc, on=['ym', 'stock_code', 'market'])
        del df['ym']
        return df

    @property
    def INDEX_COLUMNS(self):
        return ['date', 'stock_code', 'market']

    @property
    def VALUE_COLUMN(self):
        return 'value'

    @property
    def COLUMNS(self):
        return self.INDEX_COLUMNS + [self.VALUE_COLUMN]

    def get_data(self):
        return # override

    @property
    def address(self):
        return {
            'model_name': self._meta.model.__name__,
            'id': self.id,
        }

    def bulk_sync_data(self):
        data = self.get_data()
        nested = dict()
        for r in data:
            strdt = r.pop('date')
            dt = datetime.datetime.strptime(strdt, '%Y%m%d').date()
            if not nested.get(dt):
                nested[dt] = list()
            nested[dt].append(r)

        created = []
        updated = []
        for dt, records in nested.items():
            matched = VariableData.objects.filter(
                variable = self.address,
                date = dt
            )
            if matched.exists():
                m = matched.first()
                m.records = records
                updated.append(m)
            else:
                created.append(
                    VariableData(
                        variable = self.address,
                        date = dt,
                        records = records
                    )
                )
        if len(created) > 0:
            VariableData.objects.bulk_create(created)
            print(f"{len(created)} number of VariableData for {self} were created.")
        if len(updated) > 0:
            VariableData.objects.bulk_update(updated, ['records'])
            print(f"{len(updated)} number of VariableData for {self} were updated.")

        # if self.address['model_name'] != 'SingleAccount':
        self.write_file()

    @property
    def queryset(self):
        return VariableData.objects.filter(variable=self.address) #.order_by('date')

    def write_file(self):
        qs = self.queryset.all() #.order_by('date')
        records = list()
        for obj in qs:
            records += [{
                'date': obj.date.strftime('%Y%m%d'),
                **r
            } for r in obj.records]
        records = sorted(records, key=lambda r: (r['date'], r['stock_code']))
        files_to_zip = [{
            'name': self.csvfile_name,
            'file': convert_records_to_csv(records)
        }]
        zf = create_zipfile(files_to_zip)
        self.file.save(self.zipfile_name, zf)
        self.url = self.file.url
        self.save()
        print(f"{self.zipfile_name} was saved on cloud storage.")

    @property
    def csvfile_name(self):
        return f"{self.name}_panel.csv"

    @property
    def zipfile_name(self):
        return f"{self.name}_panel.zip"


class SingleAccount(Variable):
    client = models.OneToOneField(
        SingleAccountClient,
        related_name = 'variable',
        on_delete = models.CASCADE,
    )
    objects = SingleAccountManager()

    class Meta:
        db_table = 'single_account'

    def __str__(self):
        return self.client.__str__()

    def get_data(self):
        value_column = 'value' if self.client.fs_div == 'BS' else 'value_y'
        data = list()
        _data = self.client.query()
        for _d in _data:
            v = _d.get(value_column)
            if v:
                data.append({
                    **{c: _d[c] for c in self.INDEX_COLUMNS},
                    'value': int(v)
                })
        return data


class MixedAccount(Variable):
    # [{'model_name': model_name, 'id': object_id}]
    ordered_single_accounts = models.JSONField(default=DEFAULT_LIST)
    objects = MixedAccountManager()

    class Meta:
        db_table = 'mixed_account'

    def __str__(self):
        return self.capitalize_name()

    def get_data(self):
        ls_dfa = list()
        for i, vconf in enumerate(self.ordered_single_accounts):
            dfa = self.import_variable_data(vconf)
            dfa['priority'] = i
            ls_dfa.append(dfa)
        df = pd.concat(ls_dfa, axis=0)
        df = df.sort_values(['stock_code', 'date', 'priority'])
        df = df.drop_duplicates(['stock_code', 'date'])
        return df[self.COLUMNS].to_dict(orient='records')


class AccountRatio(Variable):
    # {'model_name': model_name, 'id': obj_id}
    numerator = models.JSONField(default=DEFAULT_DICT)
    denominator = models.JSONField(default=DEFAULT_DICT)
    objects = AccountRatioManager()

    class Meta:
        db_table = 'account_ratio'

    def __str__(self):
        return self.capitalize_name()

    def get_data(self):
        df_num = self.import_variable_data(self.numerator)
        df_num = df_num.rename(columns={'value': 'numerator'})
        df_den = self.import_variable_data(self.denominator)
        df_den = df_den.rename(columns={'value': 'denominator'})
        df = df_num.merge(df_den, on=self.INDEX_COLUMNS)
        df[self.VALUE_COLUMN] = df.numerator / df.denominator
        df = df.copy().dropna(subset='value')
        df = df.replace(np.nan, None)
        return df[self.COLUMNS].to_dict(orient='records')


class PriceRatio(Variable):
    numerator = models.JSONField(default=DEFAULT_DICT)
    objects = PriceRatioManager()

    class Meta:
        db_table = 'price_ratio'

    def __str__(self):
        return self.capitalize_name()

    def get_data(self):
        df = self.import_variable_data(self.numerator)
        df = df.rename(columns={'value': 'numerator'})
        df = self.append_mktcap_column(df)
        df = df.rename(columns={'mktcap': 'denominator'})
        df[self.VALUE_COLUMN] = df.numerator / df.denominator
        df = df.copy().dropna(subset='value')
        df = df.replace(np.nan, None)
        return df[self.COLUMNS].to_dict(orient='records')


class Momentum(Variable):
    near = models.IntegerField()
    far = models.IntegerField()
    objects = MomentumManager()

    class Meta:
        db_table = 'momentum'

    def __str__(self):
        return f"{self.capitalize_name()} ({self.near}/{self.far})"

    def get_data(self):
        qs_prc = StockPrice.objects.filter(is_monthend=True) #.order_by('date')
        mktcap_data = list()
        for prc in qs_prc:
            mktcap_data += prc.select_columns(columns=['mktcap'])
        df = pd.DataFrame.from_records(mktcap_data)
        df = df.sort_values(['stock_code', 'date']).reset_index(drop=True)
        dt_1 = df.groupby('stock_code').date.shift(1)
        is_big_gap = (df.date - dt_1) > '40 days'

        ln_mktcap = np.log(df.mktcap) # pd.Series
        ln_mktcap_1 = ln_mktcap.groupby(df.stock_code).shift(1)
        ln_ri_m = ln_mktcap - ln_mktcap_1
        ln_ri_m = (~is_big_gap).astype(int) * ln_ri_m
        ln_ri_m = ln_ri_m.replace(0, np.nan)
        win_size = self.far - self.near + 1
        shift_size = self.near - 1

        _value = ln_ri_m.groupby(df.stock_code).rolling(win_size).sum().values
        _value = np.exp(_value)
        df['value'] = pd.Series(_value).groupby(df.stock_code).shift(shift_size)
        df = df.copy().dropna(subset='value')
        df.date = df.date.apply(lambda s: s.strftime('%Y%m%d'))
        return df[self.COLUMNS].to_dict(orient='records')
        # PRICE_DATA_COLUMNS


class Size(Variable):
    class Meta:
        db_table = 'size'

    # a single entry table
    def __str__(self):
        return self.capitalize_name()

    def get_data(self):
        qs_prc = StockPrice.objects.filter(is_monthend=True) #.order_by('date')
        data = list()
        for prc in qs_prc:
            data += prc.select_columns(columns=['mktcap'])
        for r in data:
            r['value'] = r.pop('mktcap')
            r['date'] = r['date'].strftime('%Y%m%d')
        return data


class VariableData(models.Model):
    variable = models.JSONField(DEFAULT_DICT)
    date = models.DateField()
    records = models.JSONField(DEFAULT_LIST)

    class Meta:
        db_table = 'variable_data'


class Backtester(models.Model):
    factors = models.JSONField(default=DEFAULT_LIST)
    rebalancing_frequency = models.IntegerField(default=12)
    rebalancing_history = models.JSONField(default=DEFAULT_DICT)
    file = models.FileField(upload_to='products/factor-portfolios')
    url = models.TextField(null=True)
    objects = BacktesterManager()

    class Meta:
        db_table = 'backtester'

    def __str__(self):
        ls = self.list_evaluated_factors()
        return ' x '.join([f['variable'].__str__() for f in ls])

    def list_evaluated_factors(self):
        ls = list()
        for factor in self.factors:
            factor_cp = factor.copy()
            model_name = factor_cp.pop('model_name')
            id = factor_cp.pop('id')
            factor_cp['variable'] = eval(model_name).objects.get(id=id)
            ls.append(factor_cp)
        return ls

    def get_or_create_portfolios(self):
        if self.portfolios.exists():
            return self.portfolios.all(), False
        portfolios = list()
        ls_q = self.list_quantile_locs()
        for q in ls_q:
            if isinstance(q, int):
                portfolios.append(FactorPortfolio(backtester=self, quantile_locs=[q]))
            elif isinstance(q, tuple):
                portfolios.append(FactorPortfolio(backtester=self, quantile_locs=list(q)))
        FactorPortfolio.objects.bulk_create(portfolios)
        return self.portfolios.all(), True

    def list_quantile_locs(self):
        if len(self.factors) == 1:
            return [i for i, q in enumerate(self.factors[0]['quantiles'])][:-1]
        quantiles = [[
            i for i, q in enumerate(factor['quantiles'][:-1])
        ] for factor in self.factors]
        return list(product(*quantiles))

    @property
    def DEFAULT_STARTS_ON(self):
        return datetime.date(year=2022, month=7, day=1)

    def get_rebalancing_history(self):
        ls_rbdt = self.list_rebalancing_dates()

        # result looks like {rbdt_str: {pf_label: [entry, ...], ...}, ...}
        return {
            rbdt.strftime('%Y%m%d'): self.get_portfolio_entries_formed_on(rbdt, use_labels=True)
            for rbdt in ls_rbdt
        }

    def list_rebalancing_dates(self):
        ls_dt = list()
        max_dt = StockPrice.objects.aggregate(Max('date'))['date__max']

        # max_dt = StockPrice.objects.latest().date
        dt = self.DEFAULT_STARTS_ON - timedelta(days=1)
        while True:
            if dt > max_dt:
                break
            ls_dt.append(dt)
            dt += relativedelta(months=self.rebalancing_frequency)
        return ls_dt

    def get_portfolio_entries_formed_on(self, date, use_labels=False):
        ls_factors = self.list_evaluated_factors()
        ls_dff = list()
        for factor in ls_factors:
            _date = date - relativedelta(months=factor['lookback'])
            is_price_var = factor['variable']._meta.model.__name__ in ['Size', 'Momentum']
            if is_price_var:
                qs = factor['variable'].queryset.filter(
                    date__year = _date.year,
                    date__month = _date.month,
                )
            else:
                qs = factor['variable'].queryset.filter(
                    date__year = _date.year,
                    date__month__gt = _date.month - 3,
                    date__month__lte = _date.month
                )
            data = reduce(lambda x,y: x+y, [obj.records for obj in qs])
            data = list(filter(lambda r: r['market'] != 'KONEX', data))
            dff = pd.DataFrame.from_records(data)
            varnm = f"{factor['variable'].name}"
            dff[varnm] = pd.qcut(
                x = dff.value,
                q = factor['quantiles'],
                labels = list(range(len(factor['labels'])))
            )
            ls_dff.append(dff.set_index(['stock_code', 'market'])[varnm])
        df = pd.concat(ls_dff, axis=1).reset_index().dropna()
        valcols = [c for c in df.columns if c not in ['stock_code', 'market']]
        for c in valcols:
            df[c] = df[c].astype(int)
        s_group = df.set_index(valcols).index
        entries_by_portfolio = df.groupby(s_group).stock_code.apply(lambda s: s.tolist()).to_dict()
        if not use_labels:
            return entries_by_portfolio
        ls_qlocs = list(entries_by_portfolio.keys())
        qlocs2label = self.quantile_locs_to_label_map
        for qlocs in ls_qlocs:
            label = qlocs2label[qlocs]
            entries_by_portfolio[label] = entries_by_portfolio.pop(qlocs)

        # result looks like {pf: [entry, ...], ...}
        return entries_by_portfolio

    @property
    def quantile_locs_to_label_map(self):
        qlmap = dict()
        portfolios = self.portfolios.all()
        for pf in portfolios:
            if len(pf.quantile_locs) == 1:
                qlmap[pf.quantile_locs[0]] = pf.label
            elif len(pf.quantile_locs) > 1:
                qlmap[tuple(pf.quantile_locs)] = pf.label
        return qlmap

    @property
    def label_to_quantile_locs_map(self):
        return {v: k for k, v in self.quantile_locs_to_label_map.items()}

    def bulk_sync_data(self):
        changed_history = self.list_changes_in_rebalancing_history()
        if changed_history == dict():
            new_prices = self.detect_new_prices()
            if new_prices.exists():
                created = []
                updated = []
                for prc in new_prices:
                    _created, _updated = self.collect_updated_data_from_price_data_object(prc)
                    created += _created
                    updated += _updated
                self.save_updated_data(created=created, updated=updated)
                self.write_file()
                print(f"PortfolioData for {self} was synced to sources successfully.")
                return True
            print(f"PortfolioData for {self} has already been synced to sources.")
            return False

        self.rebalancing_history = self.get_rebalancing_history()
        self.save()

        qs_prc = StockPrice.objects.filter(date__gte=self.DATA_STARTS_ON) #.order_by('date')
        created = []
        updated = []
        for rbdt_str, d in changed_history.items():
            rbdt = datetime.datetime.strptime(rbdt_str, '%Y%m%d').date()
            start_dt, end_dt = self.get_holding_period(rbdt)
            subset = qs_prc.filter(date__gte=start_dt, date__lte=end_dt)
            if not subset.exists():
                continue
            for prc in subset:
                _created, _updated = self.collect_updated_data_from_price_data_object(
                    prc, use_rebalancing_date = rbdt
                )
                created += _created
                updated += _updated
        self.save_updated_data(created=created, updated=updated)
        self.write_file()
        print(f"PortfolioData for {self} was synced to sources successfully.")
        return True

    def list_changes_in_rebalancing_history(self):
        changed = False
        data = dict()

        # calculate new history
        # history = {rbdt_str: {label: [entry, ...], ...}, ...}
        new_history = self.get_rebalancing_history()
        label2qlocs = self.label_to_quantile_locs_map

        # if the first time, list all
        if self.rebalancing_history == dict():
            return new_history

        # else detect changes
        changed_history = dict()
        for new_rbdt_str, d in new_history.items():
            if not self.rebalancing_history.get(new_rbdt_str):
                # [CHANGE DETECTED] new rebalancing might be implemented
                changed_history[new_rbdt_str] = new_history[new_rbdt_str]
                continue
            for label, new_entries in d.items():
                old_entries = self.rebalancing_history[new_rbdt_str][label]
                created = any([e for e in new_entries if e not in old_entries])
                deleted = any([e for e in old_entries if e not in new_entries])
                if created or deleted:
                    # [CHANGE DETECTED] there are new entries or deleted entries
                    if not changed_history.get(new_rbdt_str):
                        changed_history[new_rbdt_str] = dict()
                    changed_history[new_rbdt_str][label] = new_entries
        return changed_history

    def detect_new_prices(self):
        sample_data = self.portfolios.first().data
        if sample_data.exists():
            data_latest = sample_data.aggregate(Max('date'))['date__max']
        else:
            data_latest = self.DATA_STARTS_ON
        prc_latest = StockPrice.objects.aggregate(Max('date'))['date__max']
        return StockPrice.objects.filter(date__gt=data_latest, date__lte=prc_latest) #.order_by('date')

    def get_holding_period(self, rebalancing_date):
        start_dt = rebalancing_date + timedelta(days=1)
        end_dt = rebalancing_date + relativedelta(months=self.rebalancing_frequency)
        return tuple([start_dt, end_dt])

    @property
    def DATA_STARTS_ON(self):
        return settings.PORTFOLIO_DATA_STARTS_ON

    def collect_updated_data_from_price_data_object(self, prc, **kwargs):
        created = []
        updated = []
        rbdt = kwargs.get('use_rebalancing_date')
        if not rbdt:
            rbdt = self.get_matched_rebalancing_date_on(prc.date)
        entries_by_portfolio = self.rebalancing_history[rbdt.strftime('%Y%m%d')]
        label2qlocs = self.label_to_quantile_locs_map
        mktcaps = prc.select_columns(columns=['mktcap'])
        for label, entries in entries_by_portfolio.items():
            mktcap_entries = list(filter(lambda r: r['stock_code'] in entries, mktcaps))
            sum_mktcap_entries = sum(list(map(
                lambda r: r['mktcap'], mktcap_entries
            )))
            qlocs = label2qlocs[label]
            if isinstance(qlocs, int):
                qlocs = [qlocs]
            portfolio = self.portfolios.get(quantile_locs=qlocs)
            matched_data = portfolio.data.filter(date=prc.date)
            if matched_data.exists():
                m = matched_data.first()
                if m.mktcap != sum_mktcap_entries:
                    m.mktcap = sum_mktcap_entries
                    updated.append(m)
                continue
            created.append(FactorPortfolioData(
                portfolio = portfolio,
                date = prc.date,
                mktcap = sum_mktcap_entries
            ))
        return created, updated

    def get_matched_rebalancing_date_on(self, date):
        ls_dt = self.list_rebalancing_dates()
        max_i = len(ls_dt) - 1
        for i, dt in enumerate(ls_dt):
            if (i+1) <= max_i:
                dt_next = ls_dt[i+1]
                if (date > dt) and (date <= dt_next):
                    return dt
        return ls_dt[-1]

    def save_updated_data(self, created, updated):
        if len(created) > 0:
            FactorPortfolioData.objects.bulk_create(created)
        if len(updated) > 0:
            FactorPortfolioData.objects.bulk_update(updated, ['mktcap'])

    def write_file(self):
        portfolios = self.portfolios.all()
        _records = list()
        for pf in portfolios:
            qs = pf.data.all() #.order_by('date')
            _records += [{
                'date': obj.date.strftime('%Y%m%d'),
                'label': pf.label,
                'mktcap': obj.mktcap
            } for obj in qs]
        df = pd.DataFrame.from_records(_records)
        df = df.sort_values(['label', 'date'])
        df['value'] = (df.mktcap / df.groupby('label').mktcap.shift(1) - 1) * 100
        df.value = round(df.value, 2)
        df = df[['date', 'label', 'value']]
        df = df.set_index(['date', 'label']).unstack('label').reset_index()
        df.columns = [c[0] if c[0] == 'date' else c[1] for c in df.columns]
        df = df.copy().dropna()
        records = df.to_dict(orient='records')

        files_to_zip = [{
            'name': f"{self.filename}.csv",
            'file': convert_records_to_csv(records)
        }]
        zf = create_zipfile(files_to_zip)
        self.file.save(f"{self.filename}.zip", zf)
        self.url = self.file.url
        self.save()
        print(f"{self.filename}.zip was saved on cloud storage.")

    @property
    def filename(self):
        ls = self.list_evaluated_factors()
        return f"daily_returns_of_portfolios_formed_on_{'_by_'.join([f['variable'].name for f in ls])}"

class FactorPortfolio(models.Model):
    backtester = models.ForeignKey(
        Backtester,
        related_name = 'portfolios',
        on_delete = models.CASCADE
    )
    quantile_locs = models.JSONField(default=DEFAULT_LIST)

    class Meta:
        db_table = 'factor_portfolio'

    def __str__(self):
        return f"{self.backtester.__str__()} ({self.label.upper()})"

    @property
    def label(self):
        if len(self.quantile_locs) == 1:
            loc = self.quantile_locs[0]
            return self.backtester.factors[0]['labels'][loc]
        elif len(self.quantile_locs) > 1:
            labels = list()
            for i, loc in enumerate(self.quantile_locs):
                labels.append(self.backtester.factors[i]['labels'][loc])
            return '_'.join(labels)


class FactorPortfolioData(models.Model):
    portfolio = models.ForeignKey(
        FactorPortfolio,
        related_name = 'data',
        on_delete = models.CASCADE
    )
    date = models.DateField()
    mktcap = models.BigIntegerField()

    class Meta:
        db_table = 'factor_portfolio_data'

    def __str__(self):
        return f"{self.portfolio.__str__()} {self.date.strftime('%Y-%m-%d')}"
