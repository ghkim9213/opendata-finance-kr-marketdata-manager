from django.core.management.base import BaseCommand
from api.tasks import sync_to_latest

class Command(BaseCommand):
    help = 'test'

    def handle(self, *args, **kwargs):
        sync_to_latest()
        print('batch complete.')
