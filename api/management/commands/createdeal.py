from django.core.management.base import BaseCommand
from api.hubspot import create_deal


class Command(BaseCommand):
    def handle(self, *args, **options):
        create_deal()
