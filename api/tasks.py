from .models import (
    CorpList,
    StockPrice,
    SingleAccountClient,
    OpendartZipfile,
    Backtester,
)
from .managers import VariableManager


def sync_openapi_to_latest():
    StockPrice.objects.bulk_sync()
    CorpList.objects.bulk_sync()


def sync_opendart_to_latest():
    changed = OpendartZipfile.objects.bulk_sync(return_status=True)
    if changed:
        qs = SingleAccountClient.objects.all()
        for obj in qs:
            obj.sync_to_sources()
    return changed


def sync_to_latest():
    # sync sources
    sync_openapi_to_latest()
    opendart_changed = sync_opendart_to_latest()

    # sync products
    vm = VariableManager()
    vm.bulk_sync(opendart_changed=opendart_changed)
    Backtester.objects.bulk_sync()
