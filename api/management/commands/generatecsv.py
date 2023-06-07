from django.core.management.base import BaseCommand, CommandError, CommandParser
# from api.views import save_contacts
# from api.views import save_accounts
from api.views import export_csv

class Command(BaseCommand):

    def handle(self, *args, **options):
        # save_contacts(None)
        # save_accounts(None)
        # save_salesforce_contacts(None)
        export_csv()