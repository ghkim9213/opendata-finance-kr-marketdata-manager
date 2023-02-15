from .clients import (
    CorpListApiClient,
    StockPriceApiClient,
    OpenApiResponsesXmlError,
)
from bs4 import BeautifulSoup
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from django.apps import apps
from django.conf import settings
from django.db import models
from django.db.models import Max
import requests
import datetime
import zipfile


class OpendartZipfileManager(models.Manager):
    def bulk_sync(self, return_status=True):
        ls_source = self.list_source_filenames()
        is_changed = False
        for source_fnm in ls_source:
            d = self.parse_filename(source_fnm)
            obj, created = self.get_or_create(
                identifier = d['identifier']
            )
            updated = obj.last_update < d['last_update']
            if created or updated:
                obj.last_update = d['last_update']
                obj.save()
                f = obj.download_from_source()
                obj.file.save(source_fnm, f)
                obj.bootstrap_text_files()
                if created:
                    print(f"OpendartZipfile {obj.__str__()} was created.")
                if updated:
                    print(f"OpendartZipfile {obj.__str__()} was updated.")
            is_changed = is_changed or created or updated
        if not is_changed:
            print("OpendartZipfile has already been synced to sources.")
        if return_status:
            return is_changed

    def list_source_filenames(self):
        url = 'https://opendart.fss.or.kr/disclosureinfo/fnltt/dwld/list.do'
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        atags = soup.find('table','tb01').find_all('a')
        get_filename = lambda a: a['onclick'][a['onclick'].find('(')+1:a['onclick'].find(')')].split(', ')[-1][1:-1]
        ls = [get_filename(a) for a in atags]
        return [fnm for fnm in ls if 'CE' not in fnm]

    def parse_filename(self, zipfile_name):
        parsed = zipfile_name.split('.')[0].split('_')
        return {
            'identifier': '_'.join(parsed[:3]),
            'last_update': datetime.datetime.strptime(parsed[3], '%Y%m%d%H%M%S')
        }


class CorpListManager(models.Manager):
    def bulk_sync(self, **kwargs):
        client = CorpListApiClient()
        if not self.exists():
            self.init_table()
            return
        initiate = kwargs.get('initiate')
        if initiate:
            latest = settings.PORTFOLIO_DATA_STARTS_ON
        else:
            latest = self.aggregate(Max('date'))['date__max']
            # latest = self.latest().date
        new_latest = client.verify_latest()
        if latest >= new_latest:
            print('CorpList has already been synced to sources.')
            return
        dt = latest + timedelta(days=1)
        while True:
            if dt > new_latest:
                print('CorpList was synced to sources successfully.')
                break
            if dt.weekday() in [5, 6]:
                prev_dt = dt
                dt += timedelta(days=1)
                continue
            records = client.query(params={'basDt': dt.strftime('%Y%m%d')})
            if len(records) == 0:
                prev_dt = dt
                dt += timedelta(days=1)
                continue
            obj = self.create(
                date = dt,
                records = records,
            )
            print(f"CorpList for {obj.__str__()} was created.")
            prev_dt = dt
            dt += timedelta(days=1)

    def init_table(self):
        client = CorpListApiClient()
        dt = client.MIN_DATE
        today = datetime.date.today()
        while True:
            if (dt.year == today.year) & (dt.month == today.month):
                break
            real_me = dt + relativedelta(day=31)
            me = client.verify_trading_monthend(real_me)
            records = client.query(params={'basDt': me.strftime('%Y%m%d')})
            obj = self.create(
                date = me,
                records = records,
            )
            print(f"CorpList for {obj.__str__()} was created.")
            prev_dt = dt
            dt += relativedelta(months=1)
        self.bulk_sync(initiate=True)


class StockPriceManager(models.Manager):
    def bulk_sync(self, **kwargs):
        client = StockPriceApiClient()
        if not self.exists():
            self.init_table()
            return
        initiate = kwargs.get('initiate')
        if initiate:
            latest = settings.PORTFOLIO_DATA_STARTS_ON
        else:
            latest = self.aggregate(Max('date'))['date__max']
            # latest = self.latest().date
        new_latest = client.verify_latest()
        if latest >= new_latest:
            print('StockPrice has already been synced to sources.')
            return
        dt = latest + timedelta(days=1)
        while True:
            if dt > new_latest:
                print('StockPrice was synced to sources successfully.')
                break
            if dt.weekday() in [5, 6]:
                prev_dt = dt
                dt += timedelta(days=1)
                continue
            records = client.query(params={'basDt': dt.strftime('%Y%m%d')})
            if len(records) == 0:
                prev_dt = dt
                dt += timedelta(days=1)
                continue
            # current_latest = self.latest().date
            current_latest = self.aggregate(Max('date'))['date__max']
            obj = self.create(
                date = dt,
                records = records,
            )
            if current_latest.month < dt.month:
                obj_monthend = self.get(date=current_latest)
                obj_monthend.is_monthend = True
                obj_monthend.save()
            obj.write_file()
            print(f"StockPrice for {obj.__str__()} was created.")
            prev_dt = dt
            dt += timedelta(days=1)

    def init_table(self):
        client = StockPriceApiClient()
        dt = client.MIN_DATE
        today = datetime.date.today()
        while True:
            if (dt.year == today.year) & (dt.month == today.month):
                break
            real_me = dt + relativedelta(day=31)
            me = client.verify_trading_monthend(real_me)
            records = client.query(params={'basDt': me.strftime('%Y%m%d')})
            obj = self.create(
                date = me,
                records = records,
                is_monthend = True
            )
            print(f"StockPrice for {obj.__str__()} was created.")
            prev_dt = dt
            dt += relativedelta(months=1)
        self.bulk_sync(initiate=True)


class SingleAccountClientManager(models.Manager):
    def get_or_create_using_conf(self, conf):
        client_name = ''.join([x.capitalize() for x in conf['name'].split('_')])
        obj, created = self.get_or_create(
            fs_div = conf['fs_div'],
            name = conf['name'],
            cfs = conf['cfs']
        )
        if obj.label_en == None:
            # label_en = input(f"label_en for {obj.__str__()}: ")
            obj.label_en = conf['name'].replace('_', ' ')
            obj.save()
        if obj.label_kr == None:
            label_kr = input(f"label_kr for {obj.__str__()}: ")
            obj.label_kr = label_kr
            obj.save()
        if not bool(obj.file):
            obj.sync_to_sources()
        return obj, created

    def bulk_sync(self):
        ls_outdated = self.list_outdated()
        if len(ls_outdated) == 0:
            print('No outdated single account data.')
            return
        classified = self.classify_outdated_by_sources(ls_outdated)
        self.update_outdated(classified)

    def list_outdated(self):
        ls = list()
        qs = self.all()
        for obj in qs:
            last_update_of_sources = obj.get_last_update_of_sources()
            if obj.last_update < last_update_of_sources:
                ls.append(obj)
        return ls

    def classify_outdated_by_sources(self, ls_outdated):
        classified = dict()
        for obj in ls_outdated:
            ls_sources = obj.list_sources()
            for zfid, tfid in ls_sources:
                if not classified.get(zfid):
                    classified[zfid] = dict()
                if not classified.get(zfid).get(tfid):
                    classified[zfid][tfid] = list()
                classified[zfid][tfid].append(obj)
        return classified

    def update_outdated(self, classified):
        records_by_obj = dict()
        nstd_records = list()
        zfmodel = apps.get_model('api', 'OpendartZipfile')
        tfmodel = apps.get_model('api', 'OpendartTextfile')
        for zfid, d in classified.items():
            zfobj = zfmodel.objects.get(pk=zfid)
            zf = zfobj.get_file()
            for tfid, ls_obj in d.items():
                acnt_nm_to_obj_map = {obj.name: obj for obj in ls_obj}
                tfobj = tfmodel.objects.get(pk=tfid)
                tfnm = tfobj.get_dirty_filename()
                tf = zf.open(tfnm)
                records = tfobj.get_records(use_file=tf)
                for r in records:
                    acnt_id = r.get('acnt_id')
                    if acnt_id.startswith('entity'):
                        nstd_records.append(r)
                        continue
                    acnt_nm = acnt_id.split('_')[-1]
                    matched_obj = acnt_nm_to_obj_map.get(acnt_nm)
                    if not matched_obj:
                        continue
                    if not records_by_obj.get(matched_obj):
                        records_by_obj[matched_obj] = list()
                    records_by_obj[matched_obj].append(r)
            zf.close()

        for obj, records in records_by_obj.items():
            set_std_label_kr = set(list(map(
                lambda r: r['label_kr'].replace(' ',''),
                records
            )))

            matched_nstd_records = list(filter(
                lambda r: r['label_kr'].replace(' ','') in set_std_label_kr,
                nstd_records
            ))
            records += matched_nstd_records
            obj.write_file(use_records=records)
            obj.last_update = obj.get_last_update_of_sources()
            obj.save()
            print(f"SingleAccountClient {obj.__str__()} was updated.")


class VariableManager:
    def __init__(self):
        from api.src.variable_configs import VARIABLE_CONFIGS
        self.configs = VARIABLE_CONFIGS

    def bulk_sync(self, **kwargs):
        is_changed = False
        opendart_changed = kwargs.get('opendart_changed')
        ls = list()
        for tp, ls_conf in self.configs.items():
            for conf in ls_conf:
                model = apps.get_model('api', conf['model_name'])
                if tp == 'etc':
                    var, created = model.objects.get_or_create(
                        name = conf['name'],
                        label_en = conf['label_en'],
                        label_kr = conf['label_kr'],
                    )
                else:
                    var, created = model.objects.get_or_create_using_conf(conf)
                if not (created or opendart_changed):
                    ls.append(var)
                    continue
                var.bulk_sync_data()
                if created:
                    print(f"{var} was created.")
                is_changed = is_changed or created or opendart_changed
        if not is_changed:
            print("Variable models have already been synced to sources.")
        return_list = kwargs.get('return_list')
        if return_list:
            return ls

    def list(self):
        return self.bulk_sync(return_list=True)


class SingleAccountManager(models.Manager):
    def get_or_create_using_conf(self, conf):
        client_model = apps.get_model('api', 'SingleAccountClient')
        client, created = client_model.objects.get_or_create_using_conf(conf)
        obj, created = self.get_or_create(
            name = client.name,
            label_en = conf['name'].replace('_', ' '),
            label_kr = client.label_kr,
            client = client
        )
        return obj, created


class MixedAccountManager(models.Manager):
    def get_or_create_using_conf(self, conf):
        ordered_single_accounts = list()
        for acnt_conf in conf['ordered_account_confs']:
            acnt_model_name = acnt_conf.get('model_name')
            acnt_model = apps.get_model('api', acnt_model_name)
            acnt, created = acnt_model.objects.get_or_create_using_conf(acnt_conf)
            ordered_single_accounts.append({
                'model_name': acnt_model_name,
                'id': acnt.id,
            })
        obj, created = self.get_or_create(
            name = conf['name'],
            ordered_single_accounts = ordered_single_accounts,
        )
        if obj.label_en == None:
            obj.label_en = conf['name'].replace('_', ' ')
            obj.save()
        if obj.label_kr == None:
            label_kr = input(f"label_kr for {obj.__str__()}: ")
            obj.label_kr = label_kr
            obj.save()
        return obj, created


class AccountRatioManager(models.Manager):
    def get_or_create_using_conf(self, conf):
        num_model_name = conf.get('numerator').get('model_name')
        num_model = apps.get_model('api', num_model_name)
        num, created = num_model.objects.get_or_create_using_conf(conf['numerator'])
        numerator = {'model_name': num_model_name, 'id': num.id}

        den_model_name = conf.get('denominator').get('model_name')
        den_model = apps.get_model('api', den_model_name)
        den, created = den_model.objects.get_or_create_using_conf(conf['denominator'])
        denominator = {'model_name': den_model_name, 'id': den.id}

        obj, created = self.get_or_create(
            name = conf['name'],
            numerator = numerator,
            denominator = denominator,
        )
        if obj.label_en == None:
            obj.label_en = conf['name'].replace('_', ' ')
            obj.save()
        if obj.label_kr == None:
            label_kr = input(f"label_kr for {obj.__str__()}: ")
            obj.label_kr = label_kr
            obj.save()
        return obj, created


class PriceRatioManager(models.Manager):
    def get_or_create_using_conf(self, conf):
        num_model_name = conf.get('numerator').get('model_name')
        num_model = apps.get_model('api', num_model_name)
        num, created = num_model.objects.get_or_create_using_conf(conf['numerator'])
        numerator = {'model_name': num_model_name, 'id': num.id}

        obj, created = self.get_or_create(
            name = conf['name'],
            numerator = numerator,
        )
        if obj.label_en == None:
            obj.label_en = conf['name'].replace('_', ' ')
            obj.save()
        if obj.label_kr == None:
            label_kr = input(f"label_kr for {obj.__str__()}: ")
            obj.label_kr = label_kr
            obj.save()
        return obj, created


class MomentumManager(models.Manager):
    def get_or_create_using_conf(self, conf):
        obj, created = self.get_or_create(
            name = f"prior_return_{conf['near']}_{conf['far']}",
            near = conf['near'],
            far = conf['far']
        )
        if obj.label_en == None:
            obj.label_en = f"prior return ({conf['near']}/{conf['far']})"
            obj.save()
        if obj.label_kr == None:
            obj.label_kr = f"누적수익률 ({conf['near']}/{conf['far']})"
            obj.save()
        return obj, created


class BacktesterManager(models.Manager):
    def bulk_sync(self):
        from api.src.backtester_configs import UNIVARIATE_BACKTESTER_CONFIGS
        from api.src.backtester_configs import MULTIVARIATE_BACKTESTER_CONFIGS
        for conf in UNIVARIATE_BACKTESTER_CONFIGS:
            backtester, created = self.get_or_create_univariate_using_conf(conf)
            if not backtester.portfolios.exists():
                portfolios, created = backtester.get_or_create_portfolios()
            backtester.bulk_sync_data()
        for conf in MULTIVARIATE_BACKTESTER_CONFIGS:
            backtester, created = self.get_or_create_multivariate_using_conf(conf)
            if not backtester.portfolios.exists():
                portfolios, created = backtester.get_or_create_portfolios()
            backtester.bulk_sync_data()

    def get_or_create_univariate_using_conf(self, conf):
        factor_conf = conf.copy()
        var_conf = factor_conf.pop('variable')
        variable_model_name = var_conf['model_name']
        variable_model = apps.get_model('api', variable_model_name)
        if variable_model_name in ['Size']:
            var, created = variable_model.objects.get_or_create(
                name = var_conf['name'],
                label_en = var_conf['label_en'],
                label_kr = var_conf['label_kr'],
            )
        else:
            var, created = variable_model.objects.get_or_create_using_conf(
                conf = var_conf
            )
        rebalancing_frequency = factor_conf.pop('rebalancing_frequency')
        factor = {
            'model_name': variable_model_name,
            'id': var.id,
            **factor_conf
        }
        obj, created = self.get_or_create(
            factors = [factor],
            rebalancing_frequency = rebalancing_frequency
        )
        return obj, created

    def get_or_create_multivariate_using_conf(self, conf):
        factors = list()
        for _factor_conf in conf['factors']:
            factor_conf = _factor_conf.copy()
            var_conf = factor_conf.pop('variable')
            variable_model_name = var_conf['model_name']
            variable_model = apps.get_model('api', variable_model_name)
            if variable_model_name in ['Size']:
                var, created = variable_model.objects.get_or_create(
                    name = var_conf['name'],
                    label_en = var_conf['label_en'],
                    label_kr = var_conf['label_kr'],
                )
            else:
                var, created = variable_model.objects.get_or_create_using_conf(
                    conf = var_conf
                )
            factors.append({
                'model_name': variable_model_name,
                'id': var.id,
                **factor_conf
            })
        obj, created = self.get_or_create(
            factors = factors,
            rebalancing_frequency = conf['rebalancing_frequency']
        )
        return obj, created
