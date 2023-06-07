from django.core.management.base import BaseCommand
from api.salesforce import get_access_token

class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        get_access_token()